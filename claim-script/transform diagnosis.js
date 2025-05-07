// const fs = require('fs');
// const path = require('path');

// // Resolve the path to the JSON file
// const filePath = path.resolve(__dirname, './output/Untitled-2.json');

// try {
//   // Read data from JSON file
//   const rawData = fs.readFileSync(filePath, 'utf-8');
//   const data = JSON.parse(rawData);

//   const transformDiagnosis = (diagnosisArray) => {
//     const result = [];

//     diagnosisArray.forEach((item) => {
//       const trimmedItem = item.trim();

//       // If the item starts with a lowercase letter, append it to the previous item with a comma
//       if (/^[a-z]/.test(trimmedItem) && result.length > 0) {
//         result[result.length - 1] += ', ' + trimmedItem;
//       } else {
//         // Split based on capital letters within the string
//         const splitItems = trimmedItem.match(/(?:[A-Z][^A-Z]*)/g) || [];

//         splitItems.forEach((splitItem) => {
//           const splitTrimmed = splitItem.trim();

//           // If the split item starts with lowercase, append to the last entry with a comma
//           if (/^[a-z]/.test(splitTrimmed) && result.length > 0) {
//             result[result.length - 1] += ', ' + splitTrimmed;
//           } else {
//             result.push(splitTrimmed);
//           }
//         });
//       }
//     });

//     return result;
//   };

//   // Apply transformation to each object in the array
//   data.forEach(entry => {
//     if (Array.isArray(entry.diagnosis)) {
//       entry.diagnosis = transformDiagnosis(entry.diagnosis);
//     } else if (typeof entry.diagnosis === 'string') {
//       entry.diagnosis = transformDiagnosis([entry.diagnosis]);
//     }
//   });

//   // Write the transformed data back to the JSON file
//   const outputFilePath = path.resolve(__dirname, './output/data_transformed.json');
//   fs.writeFileSync(outputFilePath, JSON.stringify(data, null, 2));

//   console.log('Data transformation complete. Output written to data_transformed.json');
// } catch (error) {
//   console.error('Error reading or parsing the JSON file:', error.message);
// } 




const fs = require('fs');
const path = require('path');

// Resolve the path to the JSON file
// const filePath = path.resolve(__dirname, './output/output.json');

async function transformDiagnosis(filePath, outputFilePath) {
  console.log("🚀 ~ transformDiagnosis ~ outputFilePath:", outputFilePath)
  console.log("🚀 ~ transformDiagnosis ~ filePath:", filePath)
  try {
    // Read data from JSON file
    const rawData = fs.readFileSync(filePath, 'utf-8');
    const data = JSON.parse(rawData);

    console.log("📂 Parsed Data Type:", typeof data);
        console.log("🔍 Parsed Data Preview:", Array.isArray(data) ? data.slice(0, 2) : data); 

    const addCommasToLowercaseEntries = (diagnosisArray) => {
      const result = [];

      diagnosisArray.forEach((item) => {
        const trimmedItem = item.trim();

        // If the item starts with a lowercase letter, append it to the previous item with a comma
        if (/^[a-z]/.test(trimmedItem) && result.length > 0) {
          result[result.length - 1] += ', ' + trimmedItem;
        } else {
          result.push(trimmedItem);
        }
      });

      return result;
    };

    const splitByCapitalLetters = (diagnosisArray) => {
      const result = [];

      diagnosisArray.forEach((item) => {
        // Split based on capital letters within the string
        const splitItems = item.match(/(?:[A-Z][^A-Z]*)/g) || [];

        splitItems.forEach((splitItem) => {
          result.push(splitItem.trim());
        });
      });

      return result;
    };

    const transformDiagnosis = (diagnosisArray) => {
      const withCommas = addCommasToLowercaseEntries(diagnosisArray);
      const finalResult = splitByCapitalLetters(withCommas);
      return finalResult;
    };

    // Apply transformation to each object in the array
    data.forEach(entry => {
      if (Array.isArray(entry.diagnosis)) {
        entry.diagnosis = transformDiagnosis(entry.diagnosis);
      } else if (typeof entry.diagnosis === 'string') {
        entry.diagnosis = transformDiagnosis([entry.diagnosis]);
      }
    });

    // Write the transformed data back to the JSON file
    fs.writeFileSync(outputFilePath, JSON.stringify(data, null, 2));

    console.log(`Data transformation complete. Output written to ${outputFilePath}`);
  } catch (error) {
    console.error('Error reading or parsing the JSON file:', error.message);
  } 
}

module.exports = {transformDiagnosis};
