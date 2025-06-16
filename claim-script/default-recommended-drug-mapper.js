/// fix-items.js
// Usage: node fix-items.js mapper.json claims.json fixedClaims.json stats.json

const fs = require('fs');

// List of itemType values that indicate drugs
const DRUG_TYPES = [
  'drugs',
  'drug',
  'hypertension',
  'diabetis mellitus',
  'gynaecology general',
  'antineoplastic',
  'chronic drugs',
];

function loadJson(path) {
  return JSON.parse(fs.readFileSync(path, 'utf8'));
}

function writeJson(path, data) {
  fs.writeFileSync(path, JSON.stringify(data, null, 2), 'utf8');
}

function normalize(str) {
  return String(str || '').trim();
}

function main(mapperPath, claimsPath, outputPath, statsPath) {
  const mapperArray = loadJson(mapperPath);
  const claims = loadJson(claimsPath);

  // Build a map from default.toLowerCase() -> recommended (may be empty)
  const mapper = new Map();
  for (const { default: def, recommended } of mapperArray) {
    const key = normalize(def).toLowerCase();
    mapper.set(key, normalize(recommended));
  }

  let totalDrugs = 0;
  let notFoundDrugs = 0;
  const notFoundSet = new Set();

  const fixedClaims = claims.map((claim) => {
    const newItems = claim.item.map((item, idx) => {
      const type = normalize(claim.itemType[idx]).toLowerCase();
      const name = normalize(item);
      if (DRUG_TYPES.includes(type)) {
        totalDrugs++;
        const rec = mapper.get(name.toLowerCase());
        if (rec) {
          return rec;
        } else {
          notFoundDrugs++;
          notFoundSet.add(name);
          return item;
        }
      }
      return item;
    });
    return { ...claim, item: newItems };
  });

  // Calculate success rate
  const successRate = totalDrugs
    ? (((totalDrugs - notFoundDrugs) / totalDrugs) * 100).toFixed(2) + '%'
    : '0.00%';

  // Write outputs
  writeJson(outputPath, fixedClaims);
  writeJson(statsPath, {
    totalDrugs,
    notFoundDrugs,
    drugSuccessRate: successRate,
    notFound: { drugs: Array.from(notFoundSet) },
  });

  console.log(
    `Processed ${totalDrugs} drug items, ${notFoundDrugs} not found. Success rate: ${successRate}`,
  );
}

if (require.main === module) {
  const [
    ,
    ,
    mapperPath = 'C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/tariff/output/MAPPPPPPPPPPPPPPPPPPPPPPPPPPP.json',
    claimsPath = 'C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/gen-claims/output/data_transformed2023_2.json',
    outputPath = 'C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/gen-claims/output/data_transformed2023_3.json',
    statsPath = 'C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/gen-claims/output/data_transformed.summary2023_3.json',
  ] = process.argv;
  if (!mapperPath || !claimsPath || !outputPath || !statsPath) {
    console.error(
      'Usage: node fix-items.js <mapper.json> <claims.json> <fixed.json> <stats.json>',
    );
    process.exit(1);
  }
  main(mapperPath, claimsPath, outputPath, statsPath);
}
