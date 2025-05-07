const fs = require('fs');

/**
 * Extracts unique "Entry: " values separately for drugs and services.
 * @param {Array} records - Array of objects.
 * @returns {Object} - { entryDrugs: [], entryServices: [] }
 */
function extractUniqueEntries(records) {
  const entryDrugs = new Set();
  const entryServices = new Set();

  records.forEach(record => {
    if (record.mapped) {
      // Extract from drugs
      if (Array.isArray(record.mapped.drugs)) {
        record.mapped.drugs.forEach(item => {
          if (typeof item.code === 'string' && item.code.startsWith('Entry: ')) {
            entryDrugs.add(item.code);
          }
        });
      }

      // Extract from services
      if (Array.isArray(record.mapped.services)) {
        record.mapped.services.forEach(item => {
          if (typeof item.code === 'string' && item.code.startsWith('Entry: ')) {
            entryServices.add(item.code);
          }
        });
      }
    }
  });

  return { entryDrugs: Array.from(entryDrugs), entryServices: Array.from(entryServices) };
}

/**
 * Reads a JSON file, extracts "Entry:" items for drugs and services, and writes them to the same output file with sections.
 * @param {string} inputFile - File path for the input JSON data.
 * @param {string} outputFile - File path for the extracted entries.
 */
function writeEntriesToFile(inputFile, outputFile) {
  console.log("🚀 ~ Processing file:", inputFile);
  
  try {
    const rawData = fs.readFileSync(inputFile, 'utf8');
    const records = JSON.parse(rawData);
    const { entryDrugs, entryServices } = extractUniqueEntries(records);

    // Construct output with sections
    let outputData = "==== DRUGS ====\n";
    outputData += entryDrugs.join('\n') + '\n\n';
    outputData += "==== SERVICES ====\n";
    outputData += entryServices.join('\n');

    fs.writeFileSync(outputFile, outputData, 'utf-8');
    console.log(`✅ Successfully written ${entryDrugs.length + entryServices.length} unique "Entry: " items to ${outputFile}`);
  } catch (err) {
    console.error('❌ Error processing file:', err);
  }
}

// Example usage:
// writeEntriesToFile('input.json', 'unique_entries.txt');

module.exports = { writeEntriesToFile };
