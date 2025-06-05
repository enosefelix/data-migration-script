// excel-to-json.js
// Usage: node excel-to-json.js claims.json lookup.json output.json

const fs = require('fs');

/**
 * Reads JSON file and parses it.
 */
function readJson(path) {
  return JSON.parse(fs.readFileSync(path, 'utf8'));
}

/**
 * Writes JS object as JSON to file.
 */
function writeJson(path, data) {
  fs.writeFileSync(path, JSON.stringify(data, null, 2), 'utf8');
}

/**
 * For each claim, replace any item that matches a lookup record's
 * missing_items_description with the lookup's drug_prescription(oceanic).
 *
 * @param {Array<Object>} claims - Array of claim objects.
 * @param {Array<Object>} lookup - Array of lookup objects.
 * @returns {Array<Object>} - Transformed claims.
 */
function replaceItems(claims, lookup) {
  // build map: missing_items_description -> drug_prescription(oceanic)
  const map = new Map();
  lookup.forEach(record => {
    const key = String(record.missing_items_description).trim();
    const val = record['drug_prescription(oceanic)'];
    if (key && val) map.set(key, val);
  });

  // transform each claim
  return claims.map(claim => {
    const newItems = claim.item.map(it => {
      const trimmed = String(it).trim();
      return map.has(trimmed) ? map.get(trimmed) : it;
    });
    return {
      ...claim,
      item: newItems
    };
  });
}

// CLI entrypoint
(async () => {
  const [,, claimsPath="C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/gen-claims/output/data_transformed.json", lookupPath="C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/tariff/output/drugs_mapped_bukola.json", outputPath="C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/gen-claims/output/data_transformed3.json"] = process.argv;
  if (!claimsPath || !lookupPath || !outputPath) {
    console.error('Usage: node excel-to-json.js <claims.json> <lookup.json> <output.json>');
    process.exit(1);
  }
  try {
    const claims = readJson(claimsPath);
    const lookup = readJson(lookupPath);
    const result = replaceItems(claims, lookup);
    writeJson(outputPath, result);
    console.log(`Wrote ${result.length} transformed claims to ${outputPath}`);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
})();
