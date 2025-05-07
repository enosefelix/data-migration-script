/* First code*/

const ExcelJS = require("exceljs");
const fs = require("fs");

// const claimData = 'claim-script/data/DECEMBER 2024 CLAIMS FOR UPLOAD.xlsx';

// // read from a file
// readExcelFile(claimData)
//   .then((jsonData) => {
//     const processedData = processJsonData(jsonData);
//     // Save JSON data to a file or process it further
//     // fs.writeFileSync(`output-${new Date().getTime()}.json`, JSON.stringify(processedData, null, 2));
//     const outputDir = 'claim-script/output';
//     if (!fs.existsSync(outputDir)) {
//       fs.mkdirSync(outputDir);
//     }
//     fs.writeFileSync(`${outputDir}/output.json`, JSON.stringify(processedData, null, 2));
//     console.log("JSON data saved successfully:", jsonData);
//   })
//   .catch((error) => {
//     console.error("Error reading excel file:", error);
//   });

// async function readExcelFile(filePath) {
//   const workbook = new ExcelJS.Workbook();
//   await workbook.xlsx.readFile(filePath);

//   const jsonData = {};
//   workbook.worksheets.map((worksheet) => {
//     const worksheetData = {
//       category: worksheet.name,
//       entries: [],
//     };

//     const headers = [];
//     worksheet.getRow(1).eachCell((cell, colNumber) => {
//       const header = cell.value;
//       if (header) {
//         headers[colNumber] = header
//           .trim()
//           .toLowerCase()
//           .split(' ')
//           .map((word, index) =>
//             index === 0 ? word : word.charAt(0).toUpperCase() + word.slice(1)
//           )
//           .join('');
//       }
//     });

//     console.log("Headers:", headers); // Log headers once

//     worksheet.eachRow({ includeEmpty: true }, (row, rowNumber) => {
//       if (rowNumber === 1) return; // Skip the header row if there is one

//       const rowData = {};
//       row.eachCell((cell, colNumber) => {
//         // Assuming first row contains column headers
//         const header = headers[colNumber]; // Use pre-extracted header
//         if (header) {
//           rowData[header] =
//             typeof cell.value === "string" ? cell.value.trim() : cell.value;
//         }
//       });
//       worksheetData.entries.push(rowData);
//     });
//     jsonData[worksheet.name] = worksheetData.entries;
//   });

//   return jsonData;
// }

// /* code for claim excel */
// const ExcelJS = require('exceljs');
// const fs = require('fs');
// const path = require('path');

// async function readExcelFile(filePath) {
//   console.log("🚀 ~ readExcelFile ~ filePath:", filePath);

//   const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);

//   const jsonData = {};
//   for await (const worksheet of workbook) {
//     const worksheetData = { category: worksheet.name, entries: [] };

//     let headers = [];
//     let lastClaim = null;

//     for await (const row of worksheet) {
//       const rowNumber = row.number;

//       if (rowNumber === 1) {
//         headers = row.values.map((header) => {
//           if (typeof header === 'string') {
//             return header.trim().toLowerCase().replace(/\s+/g, '');
//           }
//           return null;
//         });
//         continue;
//       }

//       const rowData = {};
//       row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
//         const header = headers[colNumber];
//         if (header) {
//           rowData[header] = typeof cell.value === 'string' ? cell.value.trim() : cell.value;
//         }
//       });

//       if (rowData['s/n'] && rowData['membernumber']) {
//         lastClaim = { ...rowData, item: [], itemType: [], quantity: [], cost: [], total: [] };
//         worksheetData.entries.push(lastClaim);
//       }

//       if (rowData['item'] && lastClaim) {
//         lastClaim.item.push(rowData['item']);
//         lastClaim.itemType.push(rowData['itemtype']);
//         lastClaim.quantity.push(String(rowData['quantity']));
//         lastClaim.cost.push(String(rowData['cost']));
//         lastClaim.total.push(rowData['total']);
//       }
//     }

//     jsonData[worksheet.name] = worksheetData.entries;
//   }

//   return jsonData;
// }


// function processJsonData(jsonData) {
//   const processedData = [];

//   Object.keys(jsonData).forEach((sheetName) => {
//     jsonData[sheetName].forEach((entry) => {
//       const processedEntry = processEntry(entry);
//       processedData.push(processedEntry);
//     });
//   });

//   return processedData;
// }

// function processEntry(entry) {
//   const processedEntry = {};

//   Object.keys(entry).forEach((key) => {
//     const value = entry[key];
//     if (typeof value === 'string' && value.includes('\n')) {
//       processedEntry[key] = value.split('\n').map((item) => item.trim());
//     } else if (typeof value === 'string' && value.includes(',')) {
//       const splitValues = value.match(/([^,"]+|"[^"]*")+/g).map((item) => item.trim());
//       processedEntry[key] = splitValues;
//     } else {
//       processedEntry[key] = value;
//     }
//   });

//   return processedEntry;
// }

// module.exports = { readExcelFile, processJsonData, processEntry };


/* CODE FOR GEN SHEET SEPARATING OF SAME COLUMNS */ 

/**
 * Converts an Excel serial number to a proper date string (YYYY-MM-DD).
 * @param {number} excelSerialDate - The Excel date serial number.
 * @returns {string} - Formatted date string.
 */
function convertExcelDate(excelSerialDate) {
  const excelEpoch = new Date(1899, 11, 30);
  const convertedDate = new Date(excelEpoch.getTime() + excelSerialDate * 86400000);
  return convertedDate.toISOString().split('T')[0]; // Format as YYYY-MM-DD
}

/**
 * Processes an Excel file and writes consolidated claims to a write stream.
 * 
 * @param {string} inputFilePath - Path to the Excel file.
 * @param {fs.WriteStream} writeStream - The write stream for output.
 * @param {boolean} isFirstClaim - Flag indicating if this is the first claim written.
 * @returns {Promise<boolean>} - Resolves with the updated isFirstClaim flag.
 */
async function processDecExcelFile(inputFilePath, writeStream, isFirstClaim) {
  const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(inputFilePath);
  const groupedClaims = new Map();

  for await (const worksheet of workbookReader) {
    let headers = [];
    let isHeaderRow = true;

    for await (const row of worksheet) {
      if (isHeaderRow) {
        headers = row.values
          .map((cell) => {
            if (typeof cell === 'string') {
              return cell
                .trim()
                .toLowerCase()
                .split(' ')
                .map((word, index) =>
                  index === 0 ? word : word.charAt(0).toUpperCase() + word.slice(1)
                )
                .join('');
            }
            return null;
          })
          .filter((_, index) => index > 0);

        console.log(`📝 Extracted Headers in sheet "${worksheet.name}": ${headers.join(', ')}`);
        isHeaderRow = false;
        continue;
      }

      const entry = {};
      row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
        const headerKey = headers[colNumber - 1];
        if (headerKey) {
          entry[headerKey] = (() => {
            if (cell.value === null) return '';
            if (typeof cell.value === 'object') {
              if (cell.value.result !== undefined) {
                return cell.value.result.toString().trim();
              }
              if (cell.formula) {
                return `Formula: ${cell.formula}`;
              }
            }

            // Fix Date Conversion
            if (headerKey.includes('date') && typeof cell.value === 'number') {
              return convertExcelDate(cell.value); // Convert Excel date serial number
            }

            return cell.value.toString().trim();
          })();
        }
      });

      const claimNo = entry.claimNumber;
      if (!claimNo) continue;

      // Initialize claim group if not yet present
      if (!groupedClaims.has(claimNo)) {
        groupedClaims.set(claimNo, {
          claimNumber: entry.claimNumber || '',
          memberNumber: entry.memberNumber || '',
          fullName: entry.fullName || '',
          company: entry.company || '',
          gender: entry.gender || '',
          dateOfBirth: entry.dateOfBirth || '',
          memberPlan: entry.memberPlan || '',
          totalClaimed: entry.totalClaimed || '',
          status: entry.status || '',
          serviceProvider: entry.serviceProvider || '',
          typeOfVisit: entry.typeOfVisit || '',
          dateOfConsultation: entry.dateOfConsultation || null,
          dateOfAdmission: entry.dateOfAdmission || null,
          dateOfDischarge: [],
          diagnosis: entry.diagnosis || '',
          total: [],
          item: [],
          itemType: [],
          quantity: [],
          cost: [],
          awarded: [],
          ttotal: [],
          rejected: [],
          rejectionReasons: [],
          quantityApproved: [],
          auditedBy: entry.auditedBy || ''
        });
      }

      const claimGroup = groupedClaims.get(claimNo);

      // Collect values from the current row
      claimGroup.dateOfDischarge.push(entry.dateOfDischarge || '');
      claimGroup.item.push(entry.item || '');
      claimGroup.itemType.push(entry.itemType || '');
      claimGroup.quantity.push(entry.quantity || '');
      claimGroup.cost.push(entry.cost || '');
      claimGroup.awarded.push(entry.awarded || '');
      claimGroup.rejected.push(entry.rejected || '');
      claimGroup.rejectionReasons.push(entry.rejectionReasons || '');
      claimGroup.quantityApproved.push(entry.quantityApproved || '');
      claimGroup.total.push(entry.total || '');

      // Compute ttotal for the current row: quantity * cost
      const ttotal = Number(entry.quantity) * Number(entry.cost);
      claimGroup.ttotal.push(ttotal);

      // Flush to write stream once we accumulate a batch (here: 500 claims)
      if (groupedClaims.size >= 500) {
        for (const [, claim] of groupedClaims) {
          // Process dateOfDischarge: take the first non-empty value if any
          claim.dateOfDischarge = (() => {
            const nonEmptyDates = claim.dateOfDischarge.filter(
              (date) => date && date.toLowerCase() !== 'null' && date.trim() !== ''
            );
            return nonEmptyDates.length > 0 ? nonEmptyDates[0] : null;
          })();

          // Sum all ttotal values to determine the final "claimed" amount
          claim.claimed = claim.ttotal.reduce((sum, val) => sum + val, 0);
          delete claim.ttotal;

          // Process `rejected` array
          claim.rejected = claim.rejected.map(value => {
            if (typeof value === 'object') {
              return JSON.stringify(value);
            }
            if (typeof value === 'string' && value.startsWith("Formula: ")) {
              return "0";
            }
            return value;
          }).filter(value => value !== '');

          if (!isFirstClaim) writeStream.write(',\n');
          isFirstClaim = false;
          writeStream.write(JSON.stringify(claim, null, 2));
        }
        groupedClaims.clear();
      }
    }
  }

  // Write any remaining claims
  for (const [, claim] of groupedClaims) {
    claim.dateOfDischarge = (() => {
      const nonEmptyDates = claim.dateOfDischarge.filter(
        (date) => date && date.toLowerCase() !== 'null' && date.trim() !== ''
      );
      return nonEmptyDates.length > 0 ? nonEmptyDates[0] : null;
    })();

    claim.claimed = claim.ttotal.reduce((sum, val) => sum + val, 0);
    delete claim.ttotal;

    if (!isFirstClaim) writeStream.write(',\n');
    isFirstClaim = false;
    writeStream.write(JSON.stringify(claim, null, 2));
  }

  return isFirstClaim;
}

module.exports = { processDecExcelFile };
