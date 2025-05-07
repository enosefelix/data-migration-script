const fs = require("fs");
const ExcelJS = require("exceljs");

// Function to convert Excel file to JSON
async function providerExcelToJson(inputFile, outputFile) {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(inputFile);

    let jsonData = {};

    // Loop through all sheets in the workbook
    workbook.worksheets.forEach((sheet) => {
        const sheetData = [];
        let headers = [];

        // Read each row from the sheet
        sheet.eachRow((row, rowNumber) => {
            let rowValues = row.values.slice(1); // Remove first `null` value
            
            if (rowNumber === 1) {
                // First row is headers
                headers = rowValues;
            } else {
                // Map data rows to an object with headers as keys
                let rowObject = {};
                headers.forEach((header, i) => {
                    rowObject[header] = rowValues[i] || null; // Assign values to respective headers
                });
                sheetData.push(rowObject);
            }
        });

        jsonData[sheet.name] = sheetData;
    });

    // Write JSON data to a file
    fs.writeFileSync(outputFile, JSON.stringify(jsonData, null, 4), "utf-8");
    console.log(`✅ JSON file has been saved as: ${outputFile}`);
}

// Define input and output file paths
// const inputExcelFile = "Old_CLaims_Service_Providers_Name_Code_match.xlsx"; // Change to actual file path
// const outputJsonFile = "claims_service_providers.json";

// // Execute conversion
// providerExcelToJson(inputExcelFile, outputJsonFile).catch(console.error);


module.exports = {providerExcelToJson};
