// excel-to-json.js
// Usage: node excel-to-json.js input.xlsx output.json

const ExcelJS = require('exceljs');
const fs = require('fs');

/**
 * Converts the first sheet of an Excel file directly into a JSON array.
 * @param {string} inputPath - Path to the Excel file.
 * @param {string} outputPath - Path to save the JSON file.
 */
async function excelToJson(inputPath, outputPath) {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(inputPath);
  
    // Only convert the first sheet
    const worksheet = workbook.worksheets[0];
    if (!worksheet) {
      throw new Error('No sheets found in Excel file.');
    }
  
    const rows = [];
    let headers = [];
  
    worksheet.eachRow((row, rowNumber) => {
      const values = row.values.slice(1); // skip undefined index 0
  
      if (rowNumber === 1) {
        headers = values.map(h => String(h).trim());
      } else {
        const obj = {};
        values.forEach((cell, idx) => {
          const header = headers[idx] || `Column${idx+1}`;
          obj[header] = cell;
        });
        rows.push(obj);
      }
    });
  
    fs.writeFileSync(outputPath, JSON.stringify(rows, null, 2), 'utf8');
    console.log(`Converted sheet "${worksheet.name}" to JSON array with ${rows.length} rows.`);
}

// CLI entrypoint
(async () => {
  const [,, input="C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/Mapped Drugs Excel Bukola.xlsx", output="C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/tariff/output/drugs_mapped_bukola.json"] = process.argv;
  if (!input || !output) {
    console.error('Usage: node ./claim-script/excelToJsonExtractor.js <input.xlsx> <output.json>');
    process.exit(1);
  }
  try {
    await excelToJson(input, output);
  } catch (err) {
    console.error('Error converting Excel to JSON:', err);
    process.exit(1);
  }
})();
