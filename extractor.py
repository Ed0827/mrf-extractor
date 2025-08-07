# extract.py
import gzip, ijson, csv, os

DATA = "/content/drive/MyDrive/Med Reveal SLU Launch/Data Analysis/Unprocessed full Insurance Files/BlueCross/Florida/August-25-FL/HMO/FloridaBlue_HMO_in-network-rates.json.gz"
OUT_DIR = "/content/output"
os.makedirs(OUT_DIR, exist_ok=True)

# provider_groups.csv
with open(f"{OUT_DIR}/provider_groups.csv", "w", newline="") as pgf:
    pg = csv.writer(pgf)
    pg.writerow(["billing_code","npi","tin_type","tin_value"])

    # dynamic in_network writers
    in_writers = {}

    def get_writer(bc):
        if bc not in in_writers:
            f = open(f"{OUT_DIR}/in_network_{bc}.csv","w",newline="")
            w = csv.writer(f)
            w.writerow([
               "npi","negotiated_rate","expiration_date",
               "service_code","billing_code","billing_code_type",
               "negotiation_arrangement","negotiated_type","billing_class"
            ])
            in_writers[bc] = (w,f)
        return in_writers[bc][0]

    with gzip.open(DATA, "rb") as f:
        for item in ijson.items(f, "in_network.item"):
            bc  = item["billing_code"]
            bct = item["billing_code_type"]
            na  = item["negotiation_arrangement"]

            # provider_groups
            for rate in item["negotiated_rates"]:
                for pgp in rate["provider_groups"]:
                    tin = pgp["tin"]
                    for npi in pgp["npi"]:
                        pg.writerow([bc, npi, tin["type"], tin["value"]])

            # in_network_{bc}.csv
            w = get_writer(bc)
            for rate in item["negotiated_rates"]:
                for pgp in rate["provider_groups"]:
                    for npi in pgp["npi"]:
                        for price in rate["negotiated_prices"]:
                            w.writerow([
                                npi,
                                price["negotiated_rate"],
                                price["expiration_date"],
                                "|".join(price.get("service_code", [])),
                                bc, bct, na,
                                price["negotiated_type"],
                                price["billing_class"],
                            ])

    # close all in_network files
    for _, f in in_writers.values():
        f.close()
print("Extraction complete.")
