const ExcelJS = require("exceljs");
const fs = require("fs");
const providers = require('../claim-script/tariff/output/providers.json');

const providerMap = new Map(
  providers.map(p => [ String(p.provider_id), p.facility_name ])
);

/* CODE FOR GEN SHEET SEPARATING OF SAME COLUMNS */ 
/**
 * Processes a single Excel file and writes consolidated claims to an existing write stream.
 * Instead of opening its own write stream, it uses the one passed as an argument.
 * 
 * @param {string} inputFilePath - Path to the Excel file.
 * @param {fs.WriteStream} writeStream - The write stream for output.
 * @param {boolean} isFirstClaim - Flag indicating if this is the first claim written.
 * @returns {Promise<boolean>} - Resolves with the updated isFirstClaim flag.
 */
// async function processGenExcelFile(inputFilePath, writeStream, isFirstClaim) {
//   const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(inputFilePath);
//   const groupedClaims = new Map();

//   for await (const worksheet of workbookReader) {
//     let headers = [];
//     let isHeaderRow = true;

//     for await (const row of worksheet) {
//       if (isHeaderRow) {
//         // Extract headers and normalize them
//         headers = row.values
//           .map((cell) => {
//             if (typeof cell === 'string') {
//               return cell
//                 .trim()
//                 .toLowerCase()
//                 .split(' ')
//                 .map((word, index) =>
//                   index === 0 ? word : word.charAt(0).toUpperCase() + word.slice(1)
//                 )
//                 .join('');
//             }
//             return null;
//           })
//           .filter((_, index) => index > 0);

//         console.log(`📝 Extracted Headers in sheet "${worksheet.name}": ${headers.join(', ')}`);
//         isHeaderRow = false;
//         continue;
//       }

//       const entry = {};
//       row.eachCell({ includeEmpty: true }, (cell, colNumber) => {
//         const headerKey = headers[colNumber - 1];
//         if (headerKey) {
//           entry[headerKey] = (() => {
//             if (cell.value === null) return '';
//             if (typeof cell.value === 'object' && cell.value.result !== undefined) {
//               return cell.value.result; // Use formula result if available
//             }
//             return cell.value.toString().trim();
//           })();
//         }
//       });

//       const claimNo = entry.claimno;
//       if (!claimNo) continue;

//       // Initialize claim group if not yet present
//       if (!groupedClaims.has(claimNo)) {
//         groupedClaims.set(claimNo, {
//           claimno: entry.claimno,
//           memberno: entry.memberno || '',
//           attendingOfficer: entry.attendingOfficer || '',
//           diagnosis: entry.diagnosis || '',
//           diagnosiscode: entry.diagnosiscode || '',
//           typeOfVisit: entry.typeOfVisit || '',
//           dateAdded: entry.dateAdded || '',
//           dateOfConsultation: entry.dateOfConsultation || '',
//           dateOfAdmission: entry.dateOfAdmission || '',
//           providerid: entry.providerid || '',
//           rejected: entry.rejected || '',
//           recordid: entry.recordid || '',
//           memberNumber: entry.memberNumber || '',
//           batchnumber: entry.batchnumber || '',
//           batchtotal: entry.batchtotal || '',
//           batchmonth: entry.batchmonth || '',

//           item: [],
//           serviceitemcode: [],
//           itemType: [],
//           quantity: [],
//           cost: [],
//           qtyawarded: [],
//           awarded: [],
//           // Instead of a claimed array, we now keep an array of row totals (ttotal)
//           ttotal: [],
//           dateOfDischarge: [],
//         });
//       }

//       const claimGroup = groupedClaims.get(claimNo);

//       // Collect values from the current row
//       claimGroup.dateOfDischarge.push(entry.dateOfDischarge || '');
//       claimGroup.item.push(entry.item || '');
//       claimGroup.serviceitemcode.push(entry.serviceitemcode || '');
//       claimGroup.itemType.push(entry.itemType || '');
//       claimGroup.quantity.push(entry.quantity || '');
//       claimGroup.cost.push(entry.cost || '');
//       claimGroup.qtyawarded.push(entry.qtyawarded || '');
//       claimGroup.awarded.push(entry.awarded || '');

//       // Compute ttotal for the current row: quantity * cost
//       const ttotal = Number(entry.quantity) * Number(entry.cost);
//       claimGroup.ttotal.push(ttotal);

//       // Flush to write stream once we accumulate a batch (here: 500 claims)
//       if (groupedClaims.size >= 500) {
//         for (const [, claim] of groupedClaims) {
//           // Process dateOfDischarge: take the first non-empty value if any
//           claim.dateOfDischarge = (() => {
//             const nonEmptyDates = claim.dateOfDischarge.filter(
//               (date) => date && date.toLowerCase() !== 'null' && date.trim() !== ''
//             );
//             return nonEmptyDates.length > 0 ? nonEmptyDates[0] : null;
//           })();

//           // Sum all ttotal values to determine the final "claimed" amount
//           claim.claimed = claim.ttotal.reduce((sum, val) => sum + val, 0);
//           // Optionally, you can remove the temporary ttotal array:
//           delete claim.ttotal;

//           if (!isFirstClaim) writeStream.write(',\n');
//           isFirstClaim = false;
//           writeStream.write(JSON.stringify(claim, null, 2));
//         }
//         groupedClaims.clear();
//       }
//     }
//   }

//   // Write any remaining claims from groupedClaims
//   for (const [, claim] of groupedClaims) {
//     claim.dateOfDischarge = (() => {
//       const nonEmptyDates = claim.dateOfDischarge.filter(
//         (date) => date && date.toLowerCase() !== 'null' && date.trim() !== ''
//       );
//       return nonEmptyDates.length > 0 ? nonEmptyDates[0] : null;
//     })();

//     claim.claimed = claim.ttotal.reduce((sum, val) => sum + val, 0);
//     delete claim.ttotal;

//     if (!isFirstClaim) writeStream.write(',\n');
//     isFirstClaim = false;
//     writeStream.write(JSON.stringify(claim, null, 2));
//   }

//   return isFirstClaim;
// }

// module.exports = { processGenExcelFile };







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
async function processGenExcelFile(inputFilePath, writeStream, isFirstClaim) {
   console.log("🚀 ~ processGenExcelFile ~ writeStream:", writeStream)
   console.log("🚀 ~ processGenExcelFile ~ inputFilePath:", inputFilePath)
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
        // console.log("Header Key:", headerKey, "Cell Value:", cell.value);
        if (headerKey) {
          entry[headerKey] = (() => {
            if (cell.value === null) return '';
            if (typeof cell.value === 'object') {
              if (cell.value.result !== undefined) {
                // console.log("🚀 ~ row.eachCell ~ cell.value.result.toString().trim():", cell.value.result.toString().trim())
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

            // console.log("🚀 ~ row.eachCell ~ cell.value.toString().trim():", cell.value.toString().trim())
            return cell.value.toString().trim();
          })();
        }
      });

      const claimNo = entry.process_claim_no || entry.claimno;
      if (!claimNo) continue;

      // Initialize claim group if not yet present
      if (!groupedClaims.has(claimNo)) {
        groupedClaims.set(claimNo, {
          claimNumber: entry.process_claim_no || '',
          memberNumber: entry.member_no || '',
          serviceProviderId: entry.provider_id   || '',
          typeOfVisit: entry.type_of_visit || '',
          dateOfConsultation: entry.date_of_consultation || null,
          dateOfAdmission: entry.date_of_admission || null,
          dateOfDischarge: [],
          diagnosis: [],
          item: [],
          itemType: [],
          quantity: [],
          quantityAwarded: [],
          quantityFinance: [],
          unitPriceFinance: [],
          cost: [],
          awarded: [],
          rejected: [],
          date_added: entry.date_added,
          auditStatus: entry.audit_status,
          auditTime: entry.audit_time,
          rejectionReasons: [],
          quantityApproved: [],
          auditedBy: entry.audited_by || '',
          ttotal: []
        });
      }

      const claimGroup = groupedClaims.get(claimNo);

      // Collect values from the current row
      claimGroup.dateOfDischarge.push(entry.dateOfDischarge || '');
      claimGroup.item.push(entry.item || '');
      claimGroup.itemType.push(entry.item_service || '');
      claimGroup.quantity.push(entry.qty || '');
      claimGroup.cost.push(entry.unit_price || '');
      claimGroup.awarded.push(entry.unit_price_awarded || '');
      claimGroup.rejected.push(entry.finance_decision_status || '');
      claimGroup.rejectionReasons.push(entry.finance_decision_reason || '');
      claimGroup.quantityApproved.push(entry.qty_awarded || '');
      claimGroup.quantityAwarded.push(entry.qty_awarded || '');
      claimGroup.quantityFinance.push(entry.qty_finance || '');
      claimGroup.unitPriceFinance.push(entry.unit_price_finance || '');
      // push every raw diagnosis cell into the group's buffer:
      if (entry.diagnosis) {
        // strip trailing commas + whitespace
        const cleaned = entry.diagnosis.trim().replace(/,+$/, '');
        // split on comma + optional spaces before uppercase
        const parts = cleaned
          .split(/,\s*(?=[A-Z])/)
          .map(s => s.trim())
          .filter(Boolean);
      
        const claimGroup = groupedClaims.get(claimNo);
        for (const part of parts) {
          if (!claimGroup.diagnosis.includes(part)) {
            claimGroup.diagnosis.push(part);
          }
        }
      }
      // claimGroup.total.push(entry.total || '');

      // Compute ttotal for the current row: quantity * cost
      const ttotal = Number(entry.qty) * Number(entry.unit_price);
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
          // delete claim.ttotal;

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

          claim.serviceProvider = providerMap.get(String(claim.serviceProviderId)) || 'Unknown Provider';

          console.log("🚀 ~ forawait ~ claim.serviceProvider :", claim.serviceProvider )

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

    claim.serviceProvider = providerMap.get(String(claim.serviceProviderId)) || 'Unknown Provider';
    console.log("🚀 ~ processGenExcelFile ~ claim.serviceProvider:", claim.serviceProvider)

    if (!isFirstClaim) writeStream.write(',\n');
    isFirstClaim = false;
    writeStream.write(JSON.stringify(claim, null, 2));
  }

  return isFirstClaim;
}

module.exports = { processGenExcelFile };

