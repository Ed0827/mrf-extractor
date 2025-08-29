// fast_bcbs_extract.go
// Stream-only CPT extractor (one CSV per code). Inline provider_groups only.
// Usage:
//   go run fast_bcbs_extract.go \
//     -input "/path/in-network-rates.json.gz" \
//     -out "/tmp/out" \
//     -codes "27130,27447,23472,23430,25609,29881,29827" \
//     -pigz -pigz-threads 8 -progress 750000
//
// Notes:
// - Writes: /tmp/out/csv/in_network_<CPT>.csv  (one per code)
// - Logs unresolved references: /tmp/out/unresolved_provider_references.csv
// - Case-insensitive "CPT" check.

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Item struct {
	BillingCodeType        string `json:"billing_code_type"`
	BillingCode            string `json:"billing_code"`
	NegotiationArrangement any    `json:"negotiation_arrangement"` // string or null
	NegotiatedRates        []Rate `json:"negotiated_rates"`
}

type Rate struct {
	ProviderGroups     []ProviderGroup  `json:"provider_groups"`
	ProviderReferences []json.RawMessage `json:"provider_references"` // ids only; we just count/log
	NegotiatedPrices   []Price          `json:"negotiated_prices"`
}

type ProviderGroup struct {
	TIN *struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"tin"`
	NPI any `json:"npi"` // can be []number or []string; normalize later
}

type Price struct {
	NegotiatedRate      any      `json:"negotiated_rate"`      // number or string
	ExpirationDate      string   `json:"expiration_date"`
	ServiceCode         []string `json:"service_code"`
	BillingCodeModifier []string `json:"billing_code_modifier"`
	NegotiatedType      string   `json:"negotiated_type"`
	BillingClass        string   `json:"billing_class"`
}

func main() {
	inPath := flag.String("input", "", "Path to in-network-rates.json.gz")
	outDir := flag.String("out", "/tmp/out", "Output directory")
	codeStr := flag.String("codes", "", "Comma/space-separated CPT codes to keep")
	usePigz := flag.Bool("pigz", false, "Use pigz -dc for decompression")
	pigzThreads := flag.Int("pigz-threads", 0, "Threads for pigz (-p N), 0 = pigz default")
	progressEvery := flag.Int("progress", 500000, "Row progress print cadence")
	flag.Parse()

	if *inPath == "" || *codeStr == "" {
		fmt.Fprintln(os.Stderr, "Usage: -input <file.json.gz> -out <dir> -codes \"27130,27447,...\" [options]")
		os.Exit(2)
	}

	// allowlist
	allowed := make(map[string]struct{})
	for _, tok := range strings.FieldsFunc(*codeStr, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		t := strings.TrimSpace(tok)
		if t != "" {
			allowed[t] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		fmt.Fprintln(os.Stderr, "No valid codes parsed from -codes")
		os.Exit(2)
	}

	// Prep outputs
	csvDir := filepath.Join(*outDir, "csv")
	if err := os.MkdirAll(csvDir, 0o755); err != nil {
		panic(err)
	}
	unresPath := filepath.Join(*outDir, "unresolved_provider_references.csv")
	unresF, err := os.Create(unresPath)
	if err != nil {
		panic(err)
	}
	unresW := csv.NewWriter(bufio.NewWriterSize(unresF, 1<<20))
	if err := unresW.Write([]string{"billing_code", "ref_id"}); err != nil {
		panic(err)
	}
	defer func() {
		unresW.Flush()
		unresF.Close()
	}()

	// One open CSV writer per CPT
	type writerPack struct {
		f *os.File
		w *csv.Writer
	}
	writers := map[string]*writerPack{}
	getWriter := func(code string) *writerPack {
		if wp, ok := writers[code]; ok {
			return wp
		}
		path := filepath.Join(csvDir, fmt.Sprintf("in_network_%s.csv", code))
		f, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		bw := bufio.NewWriterSize(f, 1<<20)
		w := csv.NewWriter(bw)
		header := []string{
			"npi", "tin_type", "tin_value",
			"negotiated_rate", "expiration_date", "service_code",
			"billing_code", "billing_code_type", "negotiation_arrangement",
			"negotiated_type", "billing_class", "billing_code_modifier",
		}
		if err := w.Write(header); err != nil {
			panic(err)
		}
		wp := &writerPack{f: f, w: w}
		writers[code] = wp
		return wp
	}

	// Input stream
	var r io.ReadCloser
	if *usePigz {
		args := []string{"-dc"}
		if *pigzThreads > 0 {
			args = []string{"-p", fmt.Sprint(*pigzThreads), "-dc"}
		}
		cmd := exec.Command("pigz", append(args, *inPath)...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			panic(err)
		}
		if err := cmd.Start(); err != nil {
			panic(err)
		}
		r = io.NopCloser(stdout)
		defer func() {
			stdout.Close()
			_ = cmd.Wait()
		}()
	} else {
		f, err := os.Open(*inPath)
		if err != nil {
			panic(err)
		}
		gr, err := gzip.NewReader(f)
		if err != nil {
			panic(err)
		}
		r = gr
		defer func() {
			gr.Close()
			f.Close()
		}()
	}

	dec := json.NewDecoder(bufio.NewReaderSize(r, 1<<20))
	dec.UseNumber()

	// Parse to .in_network array
	// Expect root object
	tok, err := dec.Token()
	if err != nil {
		panic(err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		panic("expected JSON object at root")
	}
	// scan keys until "in_network"
	found := false
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			panic(err)
		}
		key := keyTok.(string)
		if key == "in_network" {
			// next must be array
			tok, err = dec.Token()
			if err != nil {
				panic(err)
			}
			if d, ok := tok.(json.Delim); !ok || d != '['' {
				panic("expected in_network to be an array")
			}
			found = true
			break
		}
		// skip this value
		if err := dec.Skip(); err != nil {
			panic(err)
		}
	}
	if !found {
		panic("no in_network field found")
	}

	var (
		seenItems, keptItems, outRows int64
		skippedRefRates, skippedRefIDs int64
	)
	// iterate array items
	for dec.More() {
		var it Item
		if err := dec.Decode(&it); err != nil {
			panic(err)
		}
		seenItems++

		bct := strings.ToUpper(strings.TrimSpace(it.BillingCodeType))
		if !strings.HasPrefix(bct, "CPT") {
			continue
		}
		bc := strings.TrimSpace(it.BillingCode)
		if bc == "" {
			continue
		}
		if _, ok := allowed[bc]; !ok {
			continue
		}
		keptItems++

		na := anyToString(it.NegotiationArrangement)

		for _, rate := range it.NegotiatedRates {
			if len(rate.ProviderReferences) > 0 {
				skippedRefRates++
				for range rate.ProviderReferences {
					// We don't know the actual ref id type; log "1" per ID for visibility
					_ = unresW.Write([]string{bc, "ref_id"})
					skippedRefIDs++
				}
			}
			if len(rate.ProviderGroups) == 0 || len(rate.NegotiatedPrices) == 0 {
				continue
			}
			for _, pg := range rate.ProviderGroups {
				tinType, tinVal := "", ""
				if pg.TIN != nil {
					tinType = pg.TIN.Type
					tinVal = pg.TIN.Value
				}
				npis := normalizeNPIs(pg.NPI)
				if len(npis) == 0 {
					continue
				}
				for _, p := range rate.NegotiatedPrices {
					scodes := strings.Join(nilIfNil(p.ServiceCode), "|")
					mods := strings.Join(nilIfNil(p.BillingCodeModifier), "|")
					ntype := p.NegotiatedType
					bclass := p.BillingClass
					rateStr := anyToString(p.NegotiatedRate)
					exp := p.ExpirationDate

					for _, npi := range npis {
						wp := getWriter(bc)
						_ = wp.w.Write([]string{
							npi,
							tinType,
							tinVal,
							rateStr,
							exp,
							scodes,
							bc, bct, na,
							ntype,
							bclass,
							mods,
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
	// close array
	if tok, err = dec.Token(); err != nil {
		panic(err)
	}
	// flush/close writers
	for _, wp := range writers {
		wp.w.Flush()
		_ = wp.f.Close()
	}
	unresW.Flush()
	_ = unresF.Close()

	fmt.Println("----- SUMMARY -----")
	fmt.Printf("Total in_network items seen: %d\n", seenItems)
	fmt.Printf("CPT items kept:             %d\n", keptItems)
	fmt.Printf("in_network rows written:    %d\n", outRows)
	fmt.Printf("rates with provider_refs:   %d\n", skippedRefRates)
	fmt.Printf("unresolved ref IDs logged:  %d\n", skippedRefIDs)
	fmt.Println("✅ Done.")
}

func anyToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strings.TrimRight(strings.TrimRight(fmtFloat(t), "0"), ".")
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func fmtFloat(f float64) string {
	// avoid scientific notation for money-like numbers
	return fmt.Sprintf("%.10f", f)
}

func nilIfNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func normalizeNPIs(v any) []string {
	switch a := v.(type) {
	case nil:
		return nil
	case []any:
		out := make([]string, 0, len(a))
		for _, e := range a {
			out = append(out, anyToString(e))
		}
		return out
	case []string:
		return a
	default:
		// unexpected shape; try to coerce a single value
		return []string{anyToString(a)}
	}
}
