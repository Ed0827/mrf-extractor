// fast_bcbs_extract_r2.go
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

type Item struct {
	BillingCodeType        string `json:"billing_code_type"`
	BillingCode            string `json:"billing_code"`
	NegotiationArrangement any    `json:"negotiation_arrangement"`
	NegotiatedRates        []Rate `json:"negotiated_rates"`
}
type Rate struct {
	ProviderGroups     []ProviderGroup   `json:"provider_groups"`
	ProviderReferences []json.RawMessage `json:"provider_references"`
	NegotiatedPrices   []Price           `json:"negotiated_prices"`
}
type ProviderGroup struct {
	TIN *struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"tin"`
	NPI any `json:"npi"`
}
type Price struct {
	NegotiatedRate      any      `json:"negotiated_rate"`
	ExpirationDate      string   `json:"expiration_date"`
	ServiceCode         []string `json:"service_code"`
	BillingCodeModifier []string `json:"billing_code_modifier"`
	NegotiatedType      string   `json:"negotiated_type"`
	BillingClass        string   `json:"billing_class"`
}

func main() {
	inPath := flag.String("input", "", "Path to in-network-rates.json.gz")
	outDir := flag.String("out", "/tmp/out", "Output directory")
	codeStr := flag.String("codes", "", "Comma/space-separated CPT codes")
	usePigz := flag.Bool("pigz", false, "Use pigz -dc for decompression")
	pigzThreads := flag.Int("pigz-threads", 0, "Threads for pigz (-p N)")
	progressEvery := flag.Int("progress", 750000, "Row progress cadence")
	prefix := flag.String("prefix", "", "R2 key prefix (e.g., BCBS/August-25-PPO-SJ)")
	flag.Parse()

	if *inPath == "" || *codeStr == "" {
		fmt.Fprintln(os.Stderr, "Usage: -input <file.json.gz> -out <dir> -codes \"27130,...\" -prefix <folder> [options]")
		os.Exit(2)
	}
	account := os.Getenv("R2_ACCOUNT_ID")
	ak := os.Getenv("R2_ACCESS_KEY_ID")
	sk := os.Getenv("R2_ACCESS_KEY_SECRET")
	bucket := os.Getenv("R2_BUCKET_NAME")
	if account == "" || ak == "" || sk == "" || bucket == "" || *prefix == "" {
		fmt.Fprintln(os.Stderr, "Missing env or flags: set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_ACCESS_KEY_SECRET, R2_BUCKET_NAME and -prefix")
		os.Exit(2)
	}
	endpoint := "https://" + account + ".r2.cloudflarestorage.com"

	// allowlist
	allowed := make(map[string]struct{})
	for _, tok := range strings.FieldsFunc(*codeStr, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		if t := strings.TrimSpace(tok); t != "" {
			allowed[t] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		fmt.Fprintln(os.Stderr, "No valid codes parsed from -codes")
		os.Exit(2)
	}

	// outputs
	csvDir := filepath.Join(*outDir, "csv")
	if err := os.MkdirAll(csvDir, 0o755); err != nil { panic(err) }
	unresPath := filepath.Join(*outDir, "unresolved_provider_references.csv")
	unresF, err := os.Create(unresPath); if err != nil { panic(err) }
	unresW := csv.NewWriter(bufio.NewWriterSize(unresF, 1<<20))
	_ = unresW.Write([]string{"billing_code", "ref_id"})
	defer func() { unresW.Flush(); unresF.Close() }()

	// one CSV writer per code
	type writerPack struct{ f *os.File; w *csv.Writer }
	writers := map[string]*writerPack{}
	getWriter := func(code string) *writerPack {
		if wp, ok := writers[code]; ok { return wp }
		path := filepath.Join(csvDir, fmt.Sprintf("in_network_%s.csv", code))
		f, err := os.Create(path); if err != nil { panic(err) }
		bw := bufio.NewWriterSize(f, 1<<20)
		w := csv.NewWriter(bw)
		_ = w.Write([]string{
			"npi","tin_type","tin_value",
			"negotiated_rate","expiration_date","service_code",
			"billing_code","billing_code_type","negotiation_arrangement",
			"negotiated_type","billing_class","billing_code_modifier",
		})
		wp := &writerPack{f: f, w: w}
		writers[code] = wp
		return wp
	}

	// input stream
	var r io.ReadCloser
	if *usePigz {
		args := []string{"-dc"}
		if *pigzThreads > 0 { args = []string{"-p", fmt.Sprint(*pigzThreads), "-dc"} }
		cmd := exec.Command("pigz", append(args, *inPath)...)
		stdout, err := cmd.StdoutPipe(); if err != nil { panic(err) }
		if err := cmd.Start(); err != nil { panic(err) }
		r = io.NopCloser(stdout)
		defer func() { stdout.Close(); _ = cmd.Wait() }()
	} else {
		f, err := os.Open(*inPath); if err != nil { panic(err) }
		gr, err := gzip.NewReader(f); if err != nil { panic(err) }
		r = gr
		defer func() { gr.Close(); f.Close() }()
	}

	dec := json.NewDecoder(bufio.NewReaderSize(r, 1<<20))
	dec.UseNumber()

	// expect root object { ... "in_network": [ ... ] ... }
	expectDelim(dec, '{')
	// find "in_network"
	found := false
	for dec.More() {
		k := expectString(dec) // key
		if k == "in_network" {
			expectDelim(dec, '[')
			found = true
			break
		}
		skipValue(dec)
	}
	if !found { panic("no in_network field found") }

	var (
		seenItems, keptItems, outRows int64
		skippedRefRates, skippedRefIDs int64
	)
	for dec.More() {
		var it Item
		if err := dec.Decode(&it); err != nil { panic(err) }
		seenItems++

		bct := strings.ToUpper(strings.TrimSpace(it.BillingCodeType))
		if !strings.HasPrefix(bct, "CPT") { continue }
		bc := strings.TrimSpace(it.BillingCode)
		if bc == "" { continue }
		if _, ok := allowed[bc]; !ok { continue }
		keptItems++

		na := anyToString(it.NegotiationArrangement)

		for _, rate := range it.NegotiatedRates {
			if len(rate.ProviderReferences) > 0 {
				skippedRefRates++
				for range rate.ProviderReferences {
					_ = unresW.Write([]string{bc, "ref_id"})
					skippedRefIDs++
				}
			}
			if len(rate.ProviderGroups) == 0 || len(rate.NegotiatedPrices) == 0 {
				continue
			}
			wp := getWriter(bc)
			for _, pg := range rate.ProviderGroups {
				tinType, tinVal := "", ""
				if pg.TIN != nil { tinType = pg.TIN.Type; tinVal = pg.TIN.Value }
				npis := normalizeNPIs(pg.NPI)
				if len(npis) == 0 { continue }

				for _, p := range rate.NegotiatedPrices {
					scodes := strings.Join(nilIfNil(p.ServiceCode), "|")
					mods := strings.Join(nilIfNil(p.BillingCodeModifier), "|")
					ntype := p.NegotiatedType
					bclass := p.BillingClass
					rateStr := anyToString(p.NegotiatedRate)
					exp := p.ExpirationDate

					for _, npi := range npis {
						_ = wp.w.Write([]string{
							npi, tinType, tinVal,
							rateStr, exp, scodes,
							bc, bct, na,
							ntype, bclass, mods,
						})
						outRows++
						if *progressEvery > 0 && (outRows%int64(*progressEvery) == 0) {
							fmt.Printf("[progress] CPT items: %d  rows: %d\n", keptItems, outRows)
						}
					}
				}
			}
		}
	}
	expectDelim(dec, ']') // end of in_network

	// flush/close writers
	for _, wp := range writers { wp.w.Flush(); _ = wp.f.Close() }
	unresW.Flush(); _ = unresF.Close()

	// ---- R2 upload ----
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("auto"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, sk, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{
						URL:               endpoint,
						HostnameImmutable: true,
					}, nil
				}
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			},
		)),
	)
	if err != nil { panic(err) }
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = false })

	uploadFile := func(localPath, key string, contentType string) error {
		f, err := os.Open(localPath); if err != nil { return err }
		defer f.Close()
		_, err = s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &bucket,
			Key:         &key,
			Body:        f,
			ContentType: aws.String(contentType),
		})
		return err
	}

	// CSVs
	entries, _ := os.ReadDir(csvDir)
	for _, e := range entries {
		if e.IsDir() { continue }
		lp := filepath.Join(csvDir, e.Name())
		key := filepath.ToSlash(filepath.Join(*prefix, e.Name()))
		fmt.Println("Uploading:", "s3://"+bucket+"/"+key)
		if err := uploadFile(lp, key, "text/csv"); err != nil { panic(err) }
	}

	// unresolved (only if non-empty > header)
	if st, err := os.Stat(unresPath); err == nil && st.Size() > 20 {
		key := filepath.ToSlash(filepath.Join(*prefix, "unresolved_provider_references.csv"))
		fmt.Println("Uploading:", "s3://"+bucket+"/"+key)
		if err := uploadFile(unresPath, key, "text/csv"); err != nil { panic(err) }
	}

	fmt.Println("----- SUMMARY -----")
	// (Optional: you can also print counters you tracked)
	fmt.Println("✅ Done (local files written and uploaded to R2).")
}

// ---- helpers ----
func anyToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.10f", t), "0"), ".")
	case bool:
		if t { return "true" }
		return "false"
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}
func nilIfNil(s []string) []string { if s == nil { return []string{} }; return s }
func normalizeNPIs(v any) []string {
	switch a := v.(type) {
	case nil:
		return nil
	case []any:
		out := make([]string, 0, len(a))
		for _, e := range a { out = append(out, anyToString(e)) }
		return out
	case []string:
		return a
	default:
		return []string{anyToString(a)}
	}
}
func expectDelim(dec *json.Decoder, want rune) {
	tok, err := dec.Token()
	if err != nil { panic(err) }
	d, ok := tok.(json.Delim)
	if !ok || rune(d) != want { panic(fmt.Sprintf("expected delim %q", want)) }
}
func expectString(dec *json.Decoder) string {
	tok, err := dec.Token()
	if err != nil { panic(err) }
	s, ok := tok.(string)
	if !ok { panic("expected string key") }
	return s
}
func skipValue(dec *json.Decoder) {
	// read one value (primitive or nested) fully
	tok, err := dec.Token()
	if err != nil { panic(err) }
	if d, ok := tok.(json.Delim); ok {
		// consume nested until matching
		depth := 1
		for depth > 0 {
			tok, err = dec.Token()
			if err != nil { panic(err) }
			if d2, ok := tok.(json.Delim); ok {
				switch d2 {
				case '{', '[': depth++
				case '}', ']': depth--
				}
			}
		}
	}
}
