/**
 * WE USE THIS SCRIPT TO SEGREGATE THE CLAIMS DATA
 * FOR DIFFERENT YEARS
 */



/*** INITIAL SUGGESTION */

// split-by-month-year.js
// const ExcelJS = require('exceljs');

// async function splitByMonthYear(inputPath, outputPath) {
//   const srcWb = new ExcelJS.Workbook();
//   await srcWb.xlsx.readFile(inputPath);
//   const srcWs = srcWb.worksheets[0];

//   // Grab header row
//   const headerRow = srcWs.getRow(1).values.slice(1); // drop the dummy zero index

//   // Figure out which column is date_added
//   const dateColIndex = headerRow.findIndex(h =>
//     String(h).toLowerCase() === 'date_added'
//   ) + 1;
//   if (!dateColIndex) {
//     throw new Error('Could not find a column named "date_added" in the header');
//   }

//   // Prepare a new workbook for output
//   const outWb = new ExcelJS.Workbook();

//   // Iterate over data rows
//   srcWs.eachRow((row, rowNumber) => {
//     if (rowNumber === 1) return; // skip header

//     const cellValue = row.getCell(dateColIndex).value;
//     // ExcelJS date cells come through as JS Date objects in .value
//     if (!(cellValue instanceof Date)) return;

//     // Build sheet name, e.g. "February 2024"
//     const month = cellValue.toLocaleString('default', { month: 'long' });
//     const year  = cellValue.getFullYear();
//     const sheetName = `${month} ${year}`;

//     // Get or create that sheet
//     let ws = outWb.getWorksheet(sheetName);
//     if (!ws) {
//       ws = outWb.addWorksheet(sheetName);
//       ws.addRow(headerRow);
//     }

//     // Append this row’s values (slice off the dummy first element)
//     ws.addRow(row.values.slice(1));
//   });

//   await outWb.xlsx.writeFile(outputPath);
//   console.log(`Written split workbook to ${outputPath}`);
// }

// // Run it:
// const [,, inFile='C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/test-claim-data.xlsx', outFile='C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/test-claim-data-segregate-2024.xlsx'] = process.argv;
// splitByMonthYear(inFile, outFile).catch(err => {
//   console.error(err);
//   process.exit(1);
// });


/** NEW SUGGESTION USING STREAM */

// streaming-split-by-month-year.js
// const ExcelJS = require('exceljs');

// async function splitByMonthYear(inputPath, outputPath) {
//   // 1) Streaming reader
//   const reader = new ExcelJS.stream.xlsx.WorkbookReader(inputPath, {
//     entries: 'emit',
//     sharedStrings: 'cache',
//     styles: 'cache',
//   });

//   // 2) Streaming writer (must enable sharedStrings & styles)
//   const writer = new ExcelJS.stream.xlsx.WorkbookWriter({
//     filename: outputPath,
//     useSharedStrings: true,
//     useStyles: true,
//   });

//   const sheetMap = new Map();
//   let headerRow;

//   reader.on('worksheet', worksheet => {
//     let rowCount = 0;

//     worksheet.on('row', row => {
//       rowCount++;
//       const vals = row.values.slice(1);

//       if (rowCount === 1) {
//         headerRow = vals;
//         return;
//       }
//       if (!headerRow) return; // safety

//       // On the first data row, find the date_added column index
//       if (rowCount === 2) {
//         const idx = headerRow.findIndex(h => String(h).toLowerCase() === 'date_added');
//         if (idx < 0) throw new Error('Header "date_added" not found');
//         worksheet.dateCol = idx + 1;
//       }

//       const cell = row.getCell(worksheet.dateCol).value;
//       if (!(cell instanceof Date)) return;

//       const monthYear = `${cell.toLocaleString('default',{month:'long'})} ${cell.getFullYear()}`;
//       if (!sheetMap.has(monthYear)) {
//         const outWs = writer.addWorksheet(monthYear);
//         outWs.addRow(headerRow).commit();
//         sheetMap.set(monthYear, outWs);
//       }
//       sheetMap.get(monthYear).addRow(vals).commit();
//     });
//   });

//   await reader.read();
//   // commit all sheets
//   for (const ws of sheetMap.values()) ws.commit();
//   await writer.commit();

//   console.log(`Wrote split workbook to ${outputPath}`);
// }

// // CLI
// const [,, inFile='C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/test-claim-data.xlsx', outFile='C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/test-claim-data-segregate-2018-2019.xlsx'] = process.argv;
// splitByMonthYear(inFile, outFile).catch(err => {
//   console.error(err);
//   process.exit(1);
// });

/**
 * SEGREGATE THEM BY YEARLY EXCEL SHEETS
 * WHILE ALSO SEPARATING THEM BY MONTHS IN THE SHEET
 */

// streaming-split-by-year-and-month.js
const ExcelJS = require('exceljs');

async function splitByYearAndMonth(inputPath, outputDir = '.') {
  // 1) Streaming reader
  const reader = new ExcelJS.stream.xlsx.WorkbookReader(inputPath, {
    entries: 'emit',
    sharedStrings: 'cache',
    styles: 'cache',
  });

  // We'll track, per year:
  //  - a streaming writer
  //  - a map of monthName->worksheet
  const yearData = new Map();  
  let headerRow;

  reader.on('worksheet', worksheet => {
    let rowCount = 0;

    worksheet.on('row', row => {
      rowCount++;
      const values = row.values.slice(1);

      // Capture header on row 1
      if (rowCount === 1) {
        headerRow = values;
        return;
      }
      if (!headerRow) return; // safety

      // On the first data row, locate the date_added column index
      if (rowCount === 2) {
        const idx = headerRow.findIndex(h =>
          String(h).toLowerCase() === 'date_added'
        );
        if (idx < 0) {
          throw new Error('Header "date_added" not found');
        }
        worksheet.dateCol = idx + 1; // 1-based
      }

      // Pull the date cell
      const cell = row.getCell(worksheet.dateCol).value;
      if (!(cell instanceof Date)) {
        return; // skip rows without a real Date
      }

      const year = cell.getFullYear().toString();
      const monthName = cell.toLocaleString('default', { month: 'long' });

      // Create writer for this year if needed
      if (!yearData.has(year)) {
        const writer = new ExcelJS.stream.xlsx.WorkbookWriter({
          filename: `${outputDir}/${year}.xlsx`,
          useSharedStrings: true,
          useStyles: true,
        });
        yearData.set(year, {
          writer,
          sheets: new Map(),
        });
      }

      const { writer, sheets } = yearData.get(year);

      // Create sheet for this month if needed
      if (!sheets.has(monthName)) {
        const ws = writer.addWorksheet(monthName);
        ws.addRow(headerRow).commit();  // write header
        sheets.set(monthName, ws);
      }

      // Append this row to the correct month-sheet
      sheets.get(monthName)
        .addRow(values)
        .commit();
    });
  });

  // Wait until the read is done
  await reader.read();

  // Commit all sheets and writers
  for (const { writer, sheets } of yearData.values()) {
    // (Rows were committed as we added them; sheets don't need an extra commit call)
    await writer.commit();
  }

  console.log(`Splitting complete! Created files: ${[...yearData.keys()].map(y => y + '.xlsx').join(', ')}`);
}


// CLI support
if (require.main === module) {
  const [,, inFile='C:/Users/OCT-YAB-ENG-ENOSE/Documents/Godabeg/migrationScriptUpdateProdd/claim-script/Claims Data.xlsx', outDir] = process.argv;
  if (!inFile) {
    console.error('Usage: node streaming-split-by-year-and-month.js <input.xlsx> [outputDir]');
    process.exit(1);
  }
  splitByYearAndMonth(inFile, outDir).catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = splitByYearAndMonth;
