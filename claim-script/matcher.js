/* const fs = require('fs');

// Load JSON data from files

function  mapDrugsToGenericName(data, tariff, outputFilePath) {
  console.log("🚀 ~ mapDrugsToGenericName ~ outputFilePath:", outputFilePath)
  console.log("🚀 ~ mapDrugsToGenericName ~ tariff:", tariff)
  console.log("🚀 ~ mapDrugsToGenericName ~ data:", data)
  tariff = JSON.parse(fs.readFileSync(tariff, 'utf8'));
  data = JSON.parse(fs.readFileSync(data, 'utf8'));

  // Create lookup map for exact matching
  const tariffMap = new Map();
  tariff.forEach((drug) => {
    tariffMap.set(drug.providerdrugdescription.toLowerCase() || drug.trade_name.toLowerCase(), drug.medicinecode);
  });

  let notFoundCount = 0;
  let totalDrugs = 0;

  // Process each data entry
  const result = data.map((entry) => {
    const mapped = [];
    const ttotalMap = [];

    entry.item.forEach((item, index) => {
      if (entry.itemType[index] === 'Drugs' || entry.itemType[index] === 'Drug' || entry.itemType[index] === 'DRUG' || entry.itemType[index] === 'DRUGS') {
        totalDrugs++;
        const genericName = tariffMap.get(item.toLowerCase()) || `Entry: ${item}`;

        if (genericName === `Entry: ${item}`) {
          notFoundCount++;
        }

        mapped.push(genericName);
      } else {
        mapped.push("Generic not found");
      }
      
      // Calculate ttotal
      const ttotal = Number(entry.quantity[index]) * Number(entry.cost[index]);
      ttotalMap.push(ttotal);
    });
    
    
    entry.mapped = mapped;
    entry.ttotal = ttotalMap;

    return entry;
  });
  // console.log("🚀 ~ result ~ result:", result)

  console.log(`🔍 Total drug items: ${totalDrugs}`);
  console.log(`❌ Not Found drugs: ${notFoundCount}`);
  console.log(`✅ Match success rate: ${(((totalDrugs - notFoundCount) / totalDrugs) * 100).toFixed(2)}%`);

  // return data;
  fs.writeFileSync(outputFilePath, JSON.stringify(result, null, 2));
}

// const updatedData = mapDrugsToGenericName(data, tariff);

// Write the result to a new JSON file
// fs.writeFileSync('mapped_data.json', JSON.stringify(updatedData, null, 2));
console.log('✅ Mapped data saved to mapped_data.json');

module.exports = {mapDrugsToGenericName};
 */

/* *************************************************************************** */

/* const fs = require('fs');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');
const { chain }  = require('stream-chain');

function mapItemsToGenericNameStream(dataPath, tariffPath, servicesPath, providerJsonPath, outputFilePath) {
  console.log("🚀 ~ mapItemsToGenericNameStream ~ providerJsonPath:", providerJsonPath);
  
  const tariff = JSON.parse(fs.readFileSync(tariffPath, 'utf8'));
  const services = JSON.parse(fs.readFileSync(servicesPath, 'utf8'));
  const providers = JSON.parse(fs.readFileSync(providerJsonPath, 'utf8'));

  const tariffMap = new Map();
  tariff.forEach(drug => {
    const key = (drug.providerdrugdescription || drug.trade_name || "").trim().toLowerCase();
    if (key) tariffMap.set(key, drug.medicinecode);
  });
  
  const servicesMap = new Map();
  services.forEach(service => {
    const key = (service.provider_desc || "").trim().toLowerCase();
    if (key) {
      const code = service.provider_code || service.cpt;
      servicesMap.set(key, code);
    }
  });

  const providerMap = new Map();
  providers.forEach(provider => {
    providerMap.set(provider.provider_id.toString(), provider.facility_name);
  });

  const outputStream = fs.createWriteStream(outputFilePath);
  outputStream.write('[\n');
  let isFirst = true;

  const pipeline = chain([
    fs.createReadStream(dataPath),
    parser(),
    streamArray(),
    async ({ key, value: entry }) => {
      const mappedDrugs = [];
      const mappedServices = [];
      const providerServices = [];
      const ttotalMap = [];

      // Ensure provider ID is a string
      const providerID = entry.providerid ? entry.providerid.toString() : null;

      // Assign serviceProvider BEFORE iterating over items
      entry.serviceProvider = providerMap.get(providerID) || `Entry: ${entry.providerid}`;
      if(entry.serviceProvider.startsWith("Entry: ")) {
        providerServices.push(entry.serviceProvider);
      }

      entry.item.forEach((item, index) => {
        const ttotal = Number(entry.quantity[index]) * Number(entry.cost[index]);
        ttotalMap.push(ttotal);

        const normalizedItem = item.trim().toLowerCase();
        const itemType = entry.itemType[index];

        if (itemType && ['drugs', 'drug', 'hypertension', 'diabetis mellitus', 'gynaecology general', 'antineoplastic'].includes(itemType.toLowerCase())) {
          const genericName = tariffMap.get(normalizedItem) || `Entry: ${item}`;
          mappedDrugs.push(genericName);
        } else {
          const genericName = servicesMap.get(normalizedItem) || `Entry: ${item}`;
          mappedServices.push(genericName);
        }
      });

      entry.mapped = { drugs: mappedDrugs, services: mappedServices, providers: providerServices};
      entry.ttotal = ttotalMap;

      if (!isFirst) {
        outputStream.write(',\n');
      } else {
        isFirst = false;
      }
      outputStream.write('  ' + JSON.stringify(entry, null, 2).replace(/\n/g, '\n  '));
    }
  ]);

  pipeline.on('data', () => {});

  pipeline.on('end', () => {
    outputStream.write('\n]\n');
    outputStream.end();
    console.log(`Mapped data saved to ${outputFilePath}`);
  });
  
  pipeline.on('error', (err) => {
    console.error("Error during streaming:", err);
  });
}

module.exports = { mapItemsToGenericNameStream };



const fs = require('fs');

function mapItemsToGenericName(dataPath, tariffPath, servicesPath, outputFilePath) {
  console.log("Output file path:", outputFilePath);
  
  // Load JSON data from files
  const tariff = JSON.parse(fs.readFileSync(tariffPath, 'utf8'));
  const services = JSON.parse(fs.readFileSync(servicesPath, 'utf8'));
  const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));

  // Create lookup map for drugs using tariff.json
  const tariffMap = new Map();
  tariff.forEach((drug) => {
    // Normalize key: trim whitespace and convert to lowercase
    const key = (drug.providerdrugdescription || drug.trade_name || "").trim().toLowerCase();
    if (key) {
      tariffMap.set(key, drug.medicinecode);
    }
  });

  // Create lookup map for services using services.json
  const servicesMap = new Map();
  services.forEach((service) => {
    // Normalize key: trim whitespace and convert to lowercase using provider_desc
    const key = (service.provider_desc || "").trim().toLowerCase();
    if (key) {
      // Use provider_code if available, otherwise fall back to cpt
      const code = service.provider_code || service.cpt;
      servicesMap.set(key, code);
      console.log("Service mapping: key:", key, "code:", code);
    }
  });

  // Counters and sets for tracking not found items (unique)
  let totalDrugs = 0, notFoundDrugs = 0;
  let totalServices = 0, notFoundServices = 0;
  const notFoundDrugsSet = new Set();
  const notFoundServicesSet = new Set();
  const notFoundProvidersSet = new Set();

  // Process each data entry
  const processedEntries = data.map((entry) => {
    const mappedDrugs = [];
    const mappedServices = [];
    const providerServices = [];
    const ttotalMap = [];

  // Ensure provider ID is a string
    const providerID = entry.providerid ? entry.providerid.toString() : null;

    // Assign serviceProvider BEFORE iterating over items
    entry.serviceProvider = providerMap.get(providerID) || `Entry: ${entry.providerid}`;
    if(entry.serviceProvider.startsWith("Entry: ")) {
      providerServices.push(entry.serviceProvider);
    }

    entry.item.forEach((item, index) => {
      // Calculate ttotal
      const ttotal = Number(entry.quantity[index]) * Number(entry.cost[index]);
      ttotalMap.push(ttotal);

      // Normalize the item key (trim and lowercase)
      const key = item.trim().toLowerCase();
      const itemType = entry.itemType[index];

      if (itemType && ['drugs', 'drug', 'hypertension', 'diabetis mellitus', 'gynaecology general', 'antineoplastic'].includes(itemType.toLowerCase())) {
        totalDrugs++;
        const genericName = tariffMap.get(key) || `Entry: ${item}`;
        if (genericName === `Entry: ${item}`) {
          notFoundDrugs++;
          notFoundDrugsSet.add(item);
        }
        mappedDrugs.push(genericName);
      } else {
        totalServices++;
        const genericName = servicesMap.get(key) || `Entry: ${item}`;
        if (genericName === `Entry: ${item}`) {
          notFoundServices++;
          notFoundServicesSet.add(item);
        }
        mappedServices.push(genericName);
      }
    });

    // Attach segregated mapping results to each entry
    entry.mapped = { drugs: mappedDrugs, services: mappedServices, providers: providerServices};
    entry.ttotal = ttotalMap;
  });

  // Log summary statistics
  console.log(`Total drug items: ${totalDrugs}`);
  console.log(`Not found drugs: ${notFoundDrugs}`);
  console.log(`Drug mapping success rate: ${(((totalDrugs - notFoundDrugs) / totalDrugs) * 100).toFixed(2)}%`);
  console.log(`Total service items: ${totalServices}`);
  console.log(`Not found services: ${notFoundServices}`);
  console.log(`Service mapping success rate: ${(((totalServices - notFoundServices) / totalServices) * 100).toFixed(2)}%`);

  // Create final output object with segregated not found items
  const output = {
    entries: processedEntries,
    notFound: {
      drugs: Array.from(notFoundDrugsSet),
      services: Array.from(notFoundServicesSet),
      providers: Array.from(notFoundProvidersSet)
    }
  };

  fs.writeFileSync(outputFilePath, JSON.stringify(output, null, 2));
  console.log(`Mapped data saved to ${outputFilePath}`);
}

module.exports = { mapItemsToGenericName };
 */

const fs = require("fs");
const { pipeline } = require("stream");
const { parser } = require("stream-json");
const { streamArray } = require("stream-json/streamers/StreamArray");

async function mapItemsToGenericName(
  dataPath,
  tariffPath,
  servicesPath,
  providerJsonPath,
  outputFilePath,
  notFoundFilePath
) {
  console.log("🚀 ~ outputFilePath:", outputFilePath)
  console.log("🚀 ~ providerJsonPath:", providerJsonPath)
  console.log("🚀 ~ servicesPath:", servicesPath)
  console.log("🚀 ~ tariffPath:", tariffPath)
  console.log("🚀 ~ dataPath:", dataPath)
  console.log("📌 Output file path:", outputFilePath);
  console.log("📌 Not found data will be logged to:", notFoundFilePath);
  console.log("📌 Starting Stream Processing...");

  const tariff = JSON.parse(fs.readFileSync(tariffPath, "utf8"));
  const services = JSON.parse(fs.readFileSync(servicesPath, "utf8"));
  const providers = JSON.parse(fs.readFileSync(providerJsonPath, "utf8"));

  // Create lookup maps for proper item-to-code mapping
  const tariffMap = new Map(
    tariff.map((drug) => [
      (drug.providerdrugdescription || drug.trade_name || "")
        .toString()
        .trim()
        .toLowerCase(),
      drug.medicinecode,
    ])
  );
  const servicesMap = new Map(
    services.map((service) => [
      (service.provider_desc || "").toString().trim().toLowerCase(),
      service.provider_code || service.cpt,
    ])
  );

  let totalDrugs = 0,
    notFoundDrugs = 0;
  let totalServices = 0,
    notFoundServices = 0;
  let totalProviders = providers.length,
    notFoundProviders = 0;
  const notFoundDrugsSet = new Set();
  const notFoundServicesSet = new Set();
  const notFoundProvidersSet = new Set();

  const outputStream = fs.createWriteStream(outputFilePath);
  outputStream.write("[\n");
  let isFirst = true;

  return new Promise((resolve, reject) => {
    pipeline(
      fs.createReadStream(dataPath),
      parser(),
      streamArray(),
      async function* (source) {
        for await (const { value: entry } of source) {
          console.log("🚀 ~ forawait ~ entry:", entry);
          if (!entry) continue;

          const mappedDrugs = [];
          const mappedServices = [];
          const providerServices = [];
          const ttotalMap = [];

          entry.item.forEach((item, index) => {
            const ttotal =
              Number(entry.quantity[index]) * Number(entry.cost[index]);
            ttotalMap.push(ttotal);

            const raw = item.toString();
            const key = raw.trim().toLowerCase();
            const itemType = entry.itemType[index]?.toString()
            .trim()
            .toLowerCase();
            const isDrug = [
              "drugs",
              "drug",
              "hypertension",
              "diabetis mellitus",
              "gynaecology general",
              "antineoplastic",
              "chronic drugs",
            ].includes(itemType);
            let matchedCode = null;

            if (isDrug) {
              totalDrugs++;
              matchedCode = tariffMap.get(key);
              if (!matchedCode) {
                matchedCode = `Entry: ${item}`;
                notFoundDrugsSet.add(item);
                notFoundDrugs++;
              }
              mappedDrugs.push({ item, code: matchedCode }); // Store as object
            } else {
              totalServices++;
              matchedCode = servicesMap.get(key);
              if (!matchedCode) {
                matchedCode = `Entry: ${item}`;
                notFoundServicesSet.add(item);
                notFoundServices++;
              }
              mappedServices.push({ item, code: matchedCode }); // Store as object
            }
          });

          entry.mapped = { drugs: mappedDrugs, services: mappedServices };
          entry.ttotal = ttotalMap;

          if (!isFirst) {
            outputStream.write(",\n");
          } else {
            isFirst = false;
          }
          outputStream.write(JSON.stringify(entry, null, 2));
        }
      },
      (err) => {
        outputStream.write("\n]\n");
        outputStream.end();

        if (err) {
          console.error("❌ Stream pipeline error:", err);
          reject(err);
        } else {
          console.log("✅ Stream processing completed.");

          const notFoundData = {
            totalDrugs,
            notFoundDrugs,
            drugSuccessRate: `${(((totalDrugs - notFoundDrugs) / totalDrugs) * 100).toFixed(2)}%`,
            totalServices,
            notFoundServices,
            serviceSuccessRate: `${(((totalServices - notFoundServices) / totalServices) * 100).toFixed(2)}%`,
            totalProviders,
            notFoundProviders,
            providerSuccessRate: `${totalProviders > 0 ? (((totalProviders - notFoundProviders) / totalProviders) * 100).toFixed(2) : "0.00"}%`,
            notFound: {
              drugs: Array.from(notFoundDrugsSet),
              services: Array.from(notFoundServicesSet),
              providers: Array.from(notFoundProvidersSet),
            },
          };

          fs.writeFileSync(
            notFoundFilePath,
            JSON.stringify(notFoundData, null, 2)
          );
          console.log(`✅ Not found data saved to ${notFoundFilePath}`);
          console.log(`✅ Mapped data saved to ${outputFilePath}`);

          resolve();
        }
      }
    );

    setTimeout(() => {
      console.warn("⚠️ Timeout: Pipeline may not be closing properly.");
    }, 30000);
  });
}

module.exports = { mapItemsToGenericName };
