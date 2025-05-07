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
async function processGenExcelFile(inputFilePath, writeStream, isFirstClaim) {
  const workbookReader = new ExcelJS.stream.xlsx.WorkbookReader(inputFilePath);
  const groupedClaims = new Map();

  for await (const worksheet of workbookReader) {
    let headers = [];
    let isHeaderRow = true;

    for await (const row of worksheet) {
      if (isHeaderRow) {
        // Extract headers and normalize them
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
            if (typeof cell.value === 'object' && cell.value.result !== undefined) {
              return cell.value.result; // Use formula result if available
            }
            return cell.value.toString().trim();
          })();
        }
      });

      const claimNo = entry.claimno;
      if (!claimNo) continue;

      // Initialize claim group if not yet present
      if (!groupedClaims.has(claimNo)) {
        groupedClaims.set(claimNo, {
          claimno: entry.claimno,
          memberno: entry.memberno || '',
          attendingOfficer: entry.attendingOfficer || '',
          diagnosis: entry.diagnosis || '',
          diagnosiscode: entry.diagnosiscode || '',
          typeOfVisit: entry.typeOfVisit || '',
          dateAdded: entry.dateAdded || '',
          dateOfConsultation: entry.dateOfConsultation || '',
          dateOfAdmission: entry.dateOfAdmission || '',
          providerid: entry.providerid || '',
          rejected: entry.rejected || '',
          recordid: entry.recordid || '',
          memberNumber: entry.memberNumber || '',
          batchnumber: entry.batchnumber || '',
          batchtotal: entry.batchtotal || '',
          batchmonth: entry.batchmonth || '',

          item: [],
          serviceitemcode: [],
          itemType: [],
          quantity: [],
          cost: [],
          qtyawarded: [],
          awarded: [],
          // Instead of a claimed array, we now keep an array of row totals (ttotal)
          ttotal: [],
          dateOfDischarge: [],
        });
      }

      const claimGroup = groupedClaims.get(claimNo);

      // Collect values from the current row
      claimGroup.dateOfDischarge.push(entry.dateOfDischarge || '');
      claimGroup.item.push(entry.item || '');
      claimGroup.serviceitemcode.push(entry.serviceitemcode || '');
      claimGroup.itemType.push(entry.itemType || '');
      claimGroup.quantity.push(entry.quantity || '');
      claimGroup.cost.push(entry.cost || '');
      claimGroup.qtyawarded.push(entry.qtyawarded || '');
      claimGroup.awarded.push(entry.awarded || '');

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
          // Optionally, you can remove the temporary ttotal array:
          delete claim.ttotal;

          if (!isFirstClaim) writeStream.write(',\n');
          isFirstClaim = false;
          writeStream.write(JSON.stringify(claim, null, 2));
        }
        groupedClaims.clear();
      }
    }
  }

  // Write any remaining claims from groupedClaims
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

module.exports = { processGenExcelFile };
