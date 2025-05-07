const path = require('path');
const fs = require('fs');
const { migrateClaimDec } = require('./claim-script/migrateClaim');
const { migrateClaimGen } = require('./claim-script/migrateClaimGen');
const { processGenExcelFile } = require('./claim-script/excel-gen.js');
const { processDecExcelFile } = require('./claim-script/excel.js');
const { mapItemsToGenericName } = require('./claim-script/matcher.js');
const {writeEntriesToFile} = require('./claim-script/extractor.js');
const {providerExcelToJson} = require('./claim-script/provider-data-extractor.js');
const {transformDiagnosis} = require('./claim-script/transform diagnosis.js');
const {updateClaims} = require('./claim-script/updateClaim.js')


async function main() {
  const claimType = process.argv[2]; // 'dec' or 'gen'
  console.log("🚀 ~ main ~ claimType:", claimType)
  const action = process.argv[3]; // 'process' or 'convert'
  console.log("🚀 ~ main ~ action:", action)
  const test = process.argv[4]; // test
  console.log("🚀 ~ main ~ test:", test)

  if (!claimType || !action) {
    console.error('❌ Usage: node script.js <claims-type: dec|gen> <action: process|convert>');
    process.exit(1);
  }

  const paths = {
    dec: {
      json: path.resolve(__dirname, 'claim-script/dec-claims/output/data_transformed.json'),
      json_test: path.resolve(__dirname, 'claim-script/dec-claims/output/output2.json'),
      excel: [path.resolve(__dirname, 'claim-script/dec-claims/data/DEC 24 CLAIMS UPLOAD DATA.xlsx')],
      success: path.resolve(__dirname, 'claim-script/dec-claims/output/successful_records.json'),
      failed: path.resolve(__dirname, 'claim-script/dec-claims/output/failed_records.json'),
      notFound: path.resolve(__dirname, 'claim-script/dec-claims/output/not_found_items.json'),
      match_output: path.resolve(__dirname, 'claim-script/dec-claims/output/data_transformed.json'),
      tariff: path.resolve(__dirname, 'claim-script/tariff/output/tariff.json'),
      servicesTariff: path.resolve(__dirname, 'claim-script/tariff/output/servicesTariff.json'),
      not_found_drugs: path.resolve(__dirname, 'claim-script/dec-claims/output/drugs_not_found.json'),
      provider_excel: path.resolve(__dirname, 'claim-script/tariff/data/providers.xlsx'),
      provider_json: path.resolve(__dirname, 'claim-script/tariff/output/providers.json'),
      not_found_update_path: path.resolve(__dirname, 'claim-script/dec-claims/output/update/not_found_items_for_update.json'),
      failed_records_update_path: path.resolve(__dirname, 'claim-script/dec-claims/output/update/failed_records.json'),
      successful_records_update_path: path.resolve(__dirname, 'claim-script/dec-claims/output/update/successful_records.json')
    },
    gen: {
      json: path.resolve(__dirname, 'claim-script/gen-claims/output/data_transformed.json'),
      json_test: path.resolve(__dirname, 'claim-script/gen-claims/output/output.json'),
      excel: [
        path.resolve(__dirname, 'claim-script/gen-claims/data/Oceanic - Claim Data 1.xlsx'),
        path.resolve(__dirname, 'claim-script/gen-claims/data/Oceanic - Claim Data 2.xlsx'),
        path.resolve(__dirname, 'claim-script/gen-claims/data/Oceanic - Claim Data 3.xlsx'),
      ],
      success: path.resolve(__dirname, 'claim-script/gen-claims/output/successful_records.json'),
      failed: path.resolve(__dirname, 'claim-script/gen-claims/output/failed_records.json'),
      notFound: path.resolve(__dirname, 'claim-script/gen-claims/output/not_found_items.json'),
      match_output: path.resolve(__dirname, 'claim-script/gen-claims/output/data_transformed.json'),
      tariff: path.resolve(__dirname, 'claim-script/tariff/output/tariff.json'),
      servicesTariff: path.resolve(__dirname, 'claim-script/tariff/output/servicesTariff.json'),
      not_found_drugs: path.resolve(__dirname, 'claim-script/gen-claims/output/drugs_not_found.json'),
      provider_excel: path.resolve(__dirname, 'claim-script/tariff/data/providers.xlsx'),
      provider_json: path.resolve(__dirname, 'claim-script/tariff/output/providers.json'),
      not_found_update_path: path.resolve(__dirname, 'claim-script/gen-claims/output/update/not_found_items_for_update.json'),
      failed_records_update_path: path.resolve(__dirname, 'claim-script/gen-claims/output/update/failed_records.json'),
      successful_records_update_path: path.resolve(__dirname, 'claim-script/gen-claims/output/update/successful_records.json')
    },
  };

  const selectedPaths = paths[claimType];

  if (!selectedPaths) {
    console.error(`❌ Invalid claims type "${claimType}". Use "dec" or "gen".`);
    process.exit(1);
  }

  const selectedPathsJson = test 
  ? selectedPaths.json_test 
  : selectedPaths.json;
  console.log("🚀 ~ main ~ selectedPathsJson:", selectedPathsJson)

  if (claimType === 'gen' && action === 'convert') {
    console.log("Yesuuurr");
    await convertExcelFileGen(selectedPaths.excel, selectedPathsJson);
  } else if (claimType === 'gen' && action === 'process') {
    console.log("process gen claims");
    await migrateClaimGen(selectedPathsJson, selectedPaths.success, selectedPaths.failed, selectedPaths.notFound)
  }
  else if (claimType === 'dec' && action === 'convert') {
    await convertExcelFileDec(selectedPaths.excel, selectedPathsJson);
  }
  else if (action === 'process') {
    console.warn("Processing Dec Claims");
    // console.log(`⚠️ Checking if processClaims has already been called...`);
    // console.log(`✅ processClaims is being called from: ${new Error().stack}`);
    await migrateClaimDec(selectedPathsJson, selectedPaths.success, selectedPaths.failed, selectedPaths.notFound);
  } else if (action === 'convert') {
    await convertExcelFiles(selectedPaths.excel, selectedPaths.json_test);
  } else if (action === 'match') {
    // map drugs and services in sheet
    await mapItemsToGenericName(selectedPathsJson, selectedPaths.tariff, selectedPaths.servicesTariff, selectedPaths.provider_json,selectedPaths.match_output);
  } else if (action === 'extractprovider') {
    await providerExcelToJson(selectedPaths.provider_excel, selectedPaths.provider_json )
  }else if (action === 'extract') {
    // extract not found drugs/services
    await writeEntriesToFile(selectedPathsJson, selectedPaths.not_found_drugs)
  } else if (action === 'transform') {
    await transformDiagnosis(selectedPathsJson, selectedPathsJson);
  } else if (action === 'update') {
    await updateClaims(selectedPaths.success, selectedPaths.failed_records_update_path, selectedPaths.successful_records_update_path,selectedPaths.not_found_update_path)
  } else {
    console.error(`❌ Invalid action "${action}". Use "process", "convert", "match", "extract" or "extractprovider" or "update"`);
    process.exit(1);
  }
}
main().catch((err) => {
  console.error('❌ Unexpected Error:', err);
  process.exit(1);
});

// async function convertExcelFiles(excelPaths) {
//   console.log('📂 Converting Excel Files to JSON:');
//   for (const excelPath of excelPaths) {
//     console.log(`excel path --- ${excelPath}`);
//     // Example Excel conversion logic here...
//     // You can integrate your ExcelJS logic here for reading and converting
//     readExcelFile(excelPath);
//   }
// }

const ExcelJS = require('exceljs');
async function convertExcelFiles(excelPaths, outputDir) {
  console.log('📂 Converting Excel Files to JSON:');

  for (const excelPath of excelPaths) {
    console.log(`excel path --- ${excelPath}`);
    const outputFilePath = outputDir;

    const writeStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });
    writeStream.write('{\n');

    try {
      const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(excelPath);

      let isFirstSheet = true;

      for await (const worksheet of workbookReader) {
        if (!isFirstSheet) writeStream.write(',\n');
        isFirstSheet = false;

        writeStream.write(`"${worksheet.name}": [\n`);

        let isFirstRow = true;
        let headers = [];
        let headerExtracted = false;

        for await (const row of worksheet) {
          if (!headerExtracted) {
            headers = row.values.map((cell) =>
              typeof cell === 'string'
                ? cell.trim().replace(/\s+/g, '').replace(/[^\w]/g, '').toLowerCase()
                : null
            );
            headerExtracted = true;
            continue;
          }

          const rowData = {};
          row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
            const header = headers[colNumber];
            if (header) {
              rowData[header] = cell.value !== null ? cell.value.toString().trim() : null;
            }
          });

          if (Object.keys(rowData).length > 0) {
            if (!isFirstRow) writeStream.write(',\n');
            isFirstRow = false;
            writeStream.write(JSON.stringify(rowData));
          }
        }

        writeStream.write('\n]');
      }

      writeStream.write('\n}');
      writeStream.end();

      console.log(`✅ JSON streaming saved to ${outputFilePath}`);
    } catch (error) {
      console.error(`❌ Failed to process ${excelPath}:`, error);
      writeStream.end();
    }
  }
}

module.exports = { convertExcelFiles };

/**
 * Converts multiple Excel files to a consolidated JSON file.
 * This function opens the write stream once and passes it to processGenExcelFile for each Excel file.
 *
 * @param {string[]} excelPaths - Array of Excel file paths.
 * @param {string} outputFilePath - Path to the output JSON file.
 */
async function convertExcelFileGen(excelPaths, outputFilePath) {
  console.log('📂 Converting GEN Excel Files to Consolidated JSON:');

  const writeStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });
  // Write the opening bracket for the JSON array
  writeStream.write('[');
  let isFirstClaim = true;

  try {
    for (const excelPath of excelPaths) {
      console.log(`Processing file: ${excelPath}`);
      isFirstClaim = await processGenExcelFile(excelPath, writeStream, isFirstClaim);
    }
    // Write the closing bracket after all files have been processed
    writeStream.write(']');
    writeStream.end();
    console.log(`✅ Consolidated GEN JSON saved to ${outputFilePath}`);
  } catch (error) {
    console.error(`❌ Failed to process GEN Excel files:`, error);
  }
}

/**
 * Converts multiple Excel files to a consolidated JSON file.
 * This function opens the write stream once and passes it to processGenExcelFile for each Excel file.
 *
 * @param {string[]} excelPath - Array of Excel file paths.
 * @param {string} outputFilePath - Path to the output JSON file.
 */
async function convertExcelFileDec(excelPaths, outputFilePath) {
  console.log('📂 Converting DEC Excel Files to Consolidated JSON:');


  const writeStream = fs.createWriteStream(outputFilePath, { encoding: 'utf8' });
  // Write the opening bracket for the JSON array
  writeStream.write('[');
  let isFirstClaim = false;

  try {
    console.log(`Processing file: ${excelPaths}`);
    for (const excelPath of excelPaths) {
      const normalizedPath = path.resolve(excelPath);
      isFirstClaim = await processDecExcelFile(normalizedPath, writeStream, isFirstClaim);
    }
    // Write the closing bracket after all files have been processed
    writeStream.write(']');
    writeStream.end();
    console.log(`✅ Consolidated DEC JSON saved to ${outputFilePath}`);
  } catch (error) {
    console.error(`❌ Failed to process DEC Excel files:`, error);
  }
}
