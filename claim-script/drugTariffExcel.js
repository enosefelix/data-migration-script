/* code for medicine code extraction */
const ExcelJS = require("exceljs");
const fs = require("fs");

const drugTariffData = 'claim-script/tariff/data/Oceanic Tarifff.xlsx';
const servicesTariffData = 'claim-script/tariff/data/Services tariff.xlsx';

// Read Excel File with Dynamic Header Row Support - NO Custom Logic on Data Rows
async function readExcelFile(filePath, headerRowNumber = 1) {
  const workbook = new ExcelJS.Workbook();
  await workbook.xlsx.readFile(filePath);

  const jsonData = {};
  workbook.worksheets.forEach((worksheet) => {
    const worksheetData = { category: worksheet.name, entries: [] };
    const headers = [];

    // Extract headers from the specified row
    const headerRow = worksheet.getRow(headerRowNumber);
    headerRow.eachCell((cell, colNumber) => {
      if (cell.value) {
        headers[colNumber] = cell.value
          .toString()
          .trim()
          .replace(/\s+/g, '') // Remove spaces
          .replace(/[^\w]/g, '') // Remove special characters
          .toLowerCase(); // Normalize case
      }
    });

    // Extract Data Rows (starting after headerRowNumber)
    worksheet.eachRow({ includeEmpty: false }, (row, rowNumber) => {
      if (rowNumber <= headerRowNumber) return;

      const rowData = {};
      row.eachCell((cell, colNumber) => {
        const header = headers[colNumber];
        if (header) {
          rowData[header] = cell.value !== null ? cell.value.toString().trim() : null;
        }
      });

      if (Object.keys(rowData).length > 0) {
        worksheetData.entries.push(rowData);
      }
    });

    jsonData[worksheet.name] = worksheetData.entries;
  });

  return jsonData;
}

// Process Data (No comma splitting)
function processJsonData(jsonData) {
  const processedData = [];

  Object.keys(jsonData).forEach((sheetName) => {
    jsonData[sheetName].forEach((entry) => {
      const processedEntry = processEntry(entry);
      processedData.push(processedEntry);
    });
  });

  return processedData;
}

// Clean up multi-line values only (No comma splitting)
function processEntry(entry) {
  const processedEntry = {};

  Object.keys(entry).forEach((key) => {
    const value = entry[key];
    if (typeof value === 'string' && value.includes('\n')) {
      processedEntry[key] = value.split('\n').map((item) => item.trim());
    } else {
      processedEntry[key] = value;
    }
  });

  return processedEntry;
}

// Execute Reading & Processing
// const headerRowNumber = 1; // Adjust this if needed
// readExcelFile(servicesTariffData, headerRowNumber)
//   .then((jsonData) => {
//     const processedData = processJsonData(jsonData);
//     const outputDir = 'claim-script/tariff/output';
//     const drugTariffOutput = `${outputDir}/tariff.json`;
//     const servicesTariffOutput = `${outputDir}/servicesTariff.json`;
//     if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);
//     fs.writeFileSync(servicesTariffOutput, JSON.stringify(processedData, null, 2));
//     console.log("✅ JSON data saved successfully");
//   })
//   .catch((error) => {
//     console.error("❌ Error reading Excel file:", error);
//   });



/* CODE FOR GEN SHEET*/
// const ExcelJS = require('exceljs');

// async function readExcelFile(filePath, headerRowNumber = 1) {
//   const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(filePath);

//   const jsonData = {};

//   for await (const worksheet of workbookReader) {
//     const worksheetData = { category: worksheet.name, entries: [] };
//     let headers = [];
//     let headerExtracted = false;

//     for await (const row of worksheet) {
//       if (!headerExtracted) {
//         headers = row.values.map((cell) => {
//           if (typeof cell === 'string') {
//             return cell.trim().replace(/\s+/g, '').replace(/[^\w]/g, '').toLowerCase();
//           }
//           return null;
//         });
//         headerExtracted = true;
//         continue;
//       }

//       const rowData = {};
//       row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
//         const header = headers[colNumber];
//         if (header) {
//           rowData[header] = cell.value !== null ? cell.value.toString().trim() : null;
//         }
//       });

//       if (Object.keys(rowData).length > 0) {
//         worksheetData.entries.push(rowData);
//       }
//     }

//     jsonData[worksheet.name] = worksheetData.entries;
//   }

//   return jsonData;
// }

// module.exports = { readExcelFile };
