// combine-member-provider-mapping.js
// ——————————————————————————————————————————————
// This script combines both provider‐name mapping and member‐ID lookup in one pass.
// It reads:
//   1. provider_mapping.json  (old provider → new provider)
//   2. member_mapping.json    (memberNumber → memberId)
//   3. input JSON (an array of objects, each containing at least:
//         • serviceProvider    (string)
//         • record.memberNumber (string)
//         • record.memberId     (may be empty or missing))
// It writes two outputs:
//   • member_provider_mapped.json     (all entries where both mappings succeeded or memberId already existed)
//   • member_provider_unmapped.json   (any entry where either provider or member mapping failed)
//
// Adjust the filenames in CONFIG as needed, then run with:
//   node combine-member-provider-mapping.js
// ——————————————————————————————————————————————

// combine-member-provider-mapping.js
// ——————————————————————————————————————————————
// This script reads:
//   • provider_mapping.json  (old provider → new provider)
//   • member_mapping.json    (memberNumber → memberId)
//   • combined_input.json    (array of objects, each with at least:
//         – serviceProvider    (string)
//         – record.memberNumber (string)
//         – record.memberId     (string or empty/"not found"))
//
// It outputs a single JSON file where:
//   • serviceProvider is replaced if a mapping exists; otherwise left unchanged.
//   • record.memberId is left as-is if non-empty and not "not found".
//     Otherwise, if memberNumber → memberId mapping exists, use it.
//     If no mapping, set memberId = memberNumber.
//
// Adjust the filenames in CONFIG as needed, then run with:
//   node combine-member-provider-mapping.js
// ——————————————————————————————————————————————

const fs   = require("fs");
const path = require("path");

/* ———— CONFIGURE PATHS ———— */
const PROVIDER_MAPPING_FILE = path.resolve(__dirname, "provider_mapping.json");
const MEMBER_MAPPING_FILE   = path.resolve(__dirname, "member_mapping.json");
const INPUT_FILE            = path.resolve(__dirname, "data_transformed2023_3.json");

// Output filename:
const OUTPUT_FILE = path.resolve(__dirname, "member_provider_mapped.json");
/* ——————————————————————————————— */

//
// 1) Load and parse provider_mapping.json
//
let rawProviderMapping;
try {
  rawProviderMapping = fs.readFileSync(PROVIDER_MAPPING_FILE, "utf8");
} catch (err) {
  console.error(`❌ Could not read ${PROVIDER_MAPPING_FILE}:`, err.message);
  process.exit(1);
}

let providerLookupRaw;
try {
  providerLookupRaw = JSON.parse(rawProviderMapping);
} catch (err) {
  console.error(`❌ ${PROVIDER_MAPPING_FILE} is not valid JSON:`, err.message);
  process.exit(1);
}

//
// 2) Build a “normalized” provider lookup table:
//    normalized oldProviderName → newProviderName
//
function normalizeName(name) {
  return name
    .replace(/[\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase();
}

const providerLookup = {};
Object.entries(providerLookupRaw).forEach(([oldName, newName]) => {
  const normOld = normalizeName(oldName);
  providerLookup[normOld] = newName.trim();
});

//
// 3) Load and parse member_mapping.json (memberNumber → memberId)
//
let rawMemberMapping;
try {
  rawMemberMapping = fs.readFileSync(MEMBER_MAPPING_FILE, "utf8");
} catch (err) {
  console.error(`❌ Could not read ${MEMBER_MAPPING_FILE}:`, err.message);
  process.exit(1);
}

let memberLookup;
try {
  memberLookup = JSON.parse(rawMemberMapping);
} catch (err) {
  console.error(`❌ ${MEMBER_MAPPING_FILE} is not valid JSON:`, err.message);
  process.exit(1);
}

//
// 4) Load the combined input array
//
let rawInput;
try {
  rawInput = fs.readFileSync(INPUT_FILE, "utf8");
} catch (err) {
  console.error(`❌ Could not read ${INPUT_FILE}:`, err.message);
  process.exit(1);
}

let entries;
try {
  entries = JSON.parse(rawInput);
  if (!Array.isArray(entries)) {
    throw new Error("Input JSON must be an array of objects");
  }
} catch (err) {
  console.error(`❌ ${INPUT_FILE} is not valid JSON array:`, err.message);
  process.exit(1);
}

//
// 5) Process each entry: apply both mappings in-place
//
entries.forEach(entry => {
  // — Provider replacement —
  const origProv = (entry.serviceProvider || "").toString();
  const normProv = normalizeName(origProv);
  const newProv  = providerLookup[normProv];
  if (newProv) {
    entry.serviceProvider = newProv;
  }
  // If no mapping found, leave entry.serviceProvider as-is

  // — Member ID fill-in —
  if (
    entry &&
    typeof entry.memberNumber === "string" &&
    entry.memberNumber.trim() !== ""
  ) {
    const num = entry.memberNumber.trim();
    const currentId = (entry.memberId || "").toString().trim();

    // If memberId is non-empty and not literally "not found", keep it
    if (currentId && currentId.toLowerCase() !== "not found") {
      // do nothing
    } else {
      // Attempt lookup
      const foundId = memberLookup[num];
      if (foundId) {
        entry.memberId = foundId;
      } else {
        // no mapping → set to the memberNumber itself
        entry.memberId = num;
      }
    }
  }
  // If no record.memberNumber present, leave memberId as-is (even if empty)
});

//
// 6) Write the single output file
//
fs.writeFileSync(
  OUTPUT_FILE,
  JSON.stringify(entries, null, 2),
  "utf8"
);

console.log(`✅ Wrote combined output (${entries.length} records) to ${OUTPUT_FILE}`);
