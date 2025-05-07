const db = require("../db");
const fsp = require("fs").promises;
const fs = require("fs");
const path = require("path");
const { segregateFailedRecords } = require('./segregateFailedRecords');
const JSONStream = require("JSONStream");

// const failedRecordsFile = path.resolve(__dirname, "./output/failed_records.json");
// const notFoundItemsFile = path.resolve(__dirname, "./output/not_found_items.json");
// const successfulRecordsFile = path.resolve(__dirname, "./output/successful_records.json");

const processedClaims = new Set(); // For `failed_records.json`
const processedItems = new Set();
const loggedNotFoundItems = new Set(); // For `not_found_items.json`
async function migrateClaimGen(jsonPath, successfulRecordsFile, failedRecordsFile, notFoundItemsFile) {
  console.log("🚀 ~ migrateClaimGen ~ jsonPath:", jsonPath)
  const startTime = new Date();
  // const filePath = path.resolve(__dirname, "./output/successes.json");
  const filePath = path.resolve(__dirname, "./output/output.json");
  const batchSize = parseInt(process.env.BATCH_SIZE) || 20;
  console.log(`Configured batch size: ${batchSize}`);
  const stats = {
    totalRecords: 0,
    processedRecords: 0,
    failedRecords: 0,
    skippedRecords: 0
  };

  try {
    const jsonData = await readJsonFileStream(jsonPath);
    stats.totalRecords = jsonData.length;
    console.log(`Total records to process: ${stats.totalRecords}`);

    const batches = createBatches(jsonData, batchSize);
    for (const batch of batches) {
      const batchResults = await processBatch(batch, failedRecordsFile, successfulRecordsFile, notFoundItemsFile);
      stats.processedRecords += batchResults.processed;
      stats.failedRecords += batchResults.failed;
      stats.skippedRecords += batchResults.skipped;
    }

    // Log comprehensive migration summary
    console.log(JSON.stringify({
      startTime,
      endTime: new Date(),
      duration: formatDuration(new Date() - startTime),
      stats
    }, null, 2));

    console.log("Migration complete.");
    await checkAndSegregateFailedRecords(failedRecordsFile);

  } catch (error) {
    console.error("Critical migration error:", error);
  } finally {
    process.exit();
  }
}

function createBatches(data, batchSize) {
  const batches = [];
  for (let i = 0; i < data.length; i += batchSize) {
    batches.push(data.slice(i, i + batchSize));
  }
  return batches;
}

async function processBatch(batch, failedRecordsFile, successfulRecordsFile, notFoundItemsFile) {
  const batchResults = {
    processed: 0,
    failed: 0,
    skipped: 0
  };
  for (let record of batch) {
    record = {
      ...record,
      dateOfConsultation: formatDate(record.dateOfConsultation),
      dateOfAdmission: formatDate(record.dateOfAdmission),
      dateOfDischarge: formatDate(record.dateOfDischarge),
      dateAdded: formatDate(record.dateAdded)
    }
    try {
      const {claimId, claim_number} = await processRecord(record, failedRecordsFile, successfulRecordsFile, notFoundItemsFile);
      // console.log("🚀 ~ processBatch ~ claim_number:", claim_number)
      batchResults.processed++;
      await logSuccessfulRecord(successfulRecordsFile, record, claim_number);
    } catch (error) {
      console.error(`Error processing record: ${error.message}`);
      await logFailedRecord(failedRecordsFile, {
        record, 
        error: error.message,
        stackTrace: error.stack
      });
      batchResults.failed++;
    }
  }
  return batchResults;
}

function formatDate(dateString) {
  console.log("🔍 Raw dateString:", dateString);
  if (!dateString) return null; // Return null if no date provided

  // Convert DD/MM/YYYY to MM/DD/YYYY for JS Date compatibility
  const parts = dateString.split(" ");
  console.log("🔍 Split parts:", parts);

  if (!parts[0]) return null; // If no date part is found

  const dateParts = parts[0].split("/"); // Split date from time
  console.log("🔍 Date parts:", dateParts);

  if (dateParts.length !== 3) {
    console.warn(`⚠️ Invalid date format detected: "${dateString}". Setting to NULL.`);
    return null;
  }

  // Convert DD/MM/YYYY → MM/DD/YYYY
  const formattedDate = `${dateParts[1]}/${dateParts[0]}/${dateParts[2]}`;
  console.log("🔍 Reformatted date:", formattedDate);

  const date = new Date(formattedDate);
  console.log("🔍 Parsed Date Object:", date);

  if (isNaN(date.getTime())) {
    console.warn(`⚠️ Invalid date value detected: "${dateString}". Setting to NULL.`);
    return null;
  }

  // const finalDate = date.toISOString().slice(0, 10); // Convert to YYYY-MM-DD for SQL
  // console.log("🚀 ~ formatDate ~ finalDate:", finalDate);

  return date;
}



async function readJsonFileStream(filePath) {
  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { encoding: "utf8" });
    const parser = JSONStream.parse("*"); // Parses each object in an array

    const jsonData = [];

    stream.pipe(parser);

    parser.on("data", (data) => {
      jsonData.push(data); // Process each object separately
    });

    parser.on("end", () => {
      resolve(jsonData);
    });

    parser.on("error", (err) => {
      reject(`Failed to read JSON file: ${err.message}`);
    });
  });
}

async function processRecord(record, failedRecordsFile, successfulRecordsFile, notFoundItemsFile) {
  let connection;
  try {
    connection = await db.getConnection();
    const memberInfo = await findMemberInfo(record.memberNumber);
    if (!memberInfo) {
      throw new Error(`Member not found for memberNumber: ${record.memberNumber}`);
    }

    console.log("Processing record for member:", memberInfo.mm_member_id);

    const provider = await getProviderInfo(record.serviceProvider);
  const providerId = provider[0]?.provider_id;
    if (!providerId) {
      throw new Error(`Provider not found for serviceProvider: ${record.serviceProvider}`);
    }

    const { max_claim_number } = await findLastClaimAndLotNumber();
    const { max_lot_no } = await maxLotNumber();

     try {
      await createLotStatement(memberInfo, max_lot_no, record);
    } catch (error) {
      console.error('Error in createLotStatement:', error);
      throw error;
    }

    try {
      await insertAuditAndDataEntry(max_lot_no);
    } catch (error) {
      console.error('Error in insertAuditAndDataEntry:', error);
      throw error;
    }

    let claimId = 0;
    try {
      claimId = await insertClaim(
        {
          max_claim_number,
          max_lot_no,
          memberInfo,
          serviceProvider: providerId,
          dateOfConsultation: record.dateOfConsultation,
          claimed: record.claimed,
          awarded: record.awarded,
          rejected: record.rejected,
          dateAdded: record.dateAdded,
          typeOfVisit: record.typeOfVisit,
          rejectionReason: record.rejectionReason,
          dateOfAdmission: record.dateOfAdmission,
          dateOfDischarge: record.dateOfDischarge,
        },
        record.quantity
      );
    } catch (error) {
      console.error('Error in insertClaim:', error);
      throw error;
    }

    console.log(`Claim inserted with ID: ${claimId} and claim number: ${max_claim_number}`);

    const {ms_plan_network} = memberInfo;
    const {networkId} = await getProviderNetworkId(record.serviceProvider, ms_plan_network) || 0;

    await storeItemsInTables(
      { 
        item: record.item, 
        itemType: record.itemType,
        quantity: record.quantity, 
        cost: record.cost
      },
      {
        item: record.mapped,
        quantity: record.quantity,
        cost: record.cost
      },
      {
        claimId,
        memberInfo,
        provider: providerId,
        typeOfVisit: record.typeOfVisit,
        dateOfConsultation: record.dateOfConsultation,
        diagnosis: record.diagnosis,
        networkId 
      },
      notFoundItemsFile
    );
    
    // Commit transaction if all steps succeed
    await connection.commit();
    
    return {claimId, claim_number: max_claim_number};
  } catch (error) {
     console.error("Error in processRecord:", {
      message: error.message,
      stack: error.stack,
      record: record
    });

    if (connection) {
      try {
        await connection.rollback();
        console.log("Transaction rolled back successfully");
      } catch (rollbackError) {
        console.error("Error during rollback:", rollbackError);
      }
      throw error;
    }
  } finally {
    if (connection) {
      connection.release();
    }
  }
}

async function createLotStatement(memberInfo, lot_no, record) {
  // create new statement
  console.log("Creating new statement...");
  // console.log("🚀 ~ createLotStatement ~ memberInfo:", memberInfo)

  const provider = await getProviderInfo(record.serviceProvider);
  try {

    const [result] = await db.query(
      "INSERT INTO lots SET ?",
      {
        lot_no: lot_no,
        lot_receive_date: record.dateOfConsultation,
        lot_from_date: null,
        lot_to_date: null,
        lot_type: 1, // in patient or out patient
        lot_amount: record.claimed,
        lot_provider_id: provider[0]?.provider_id,
        lot_total_claim: 1,
        lot_payment_date: null,
        lot_created_by: 1,
        lot_created: new Date().toISOString().slice(0, 10),
        lot_audit_date: null,
        lot_claimed_date: null,
        lot_claimed_status: null,
        lot_claimed_status: null,
        lot_verifiy_status: null,
        lot_audit_status: 1,
        lot_close_status: 3,
        lot_payment_status: null,
        lot_invoice_status: null,
        lot_source: 'offline',
        lot_ic_id: null,
        // lot_deleted:0,
        lot_modified_date: null,
        lot_audit_close_date: record.dateOfConsultation,
        lot_claim_close_date: null,
        lot_master_type: 1,
        lot_grp: memberInfo.ms_policy_id,
        lot_t: 'I',
        lot_remark: '',
        lot_is_chronic: 0,
        lot_management_status: 0,              
        lot_management_user_id: 0,              
        lot_closed_user_id: 0,              
        lot_merged_user_id: 0,       
      }
    )
    // console.log("🚀 ~ createLotStatement ~ result:", result)
  }catch (error) {
    throw new Error(`Failed to insert lot: ${error}`);
  }
}

async function insertAuditAndDataEntry(lot_no) {
  try {
    // insert for data entry permission
    await db.query(
      `INSERT INTO assign_lots SET ?`,
      {
        al_lot_no: lot_no,
        al_user_id: 1,
        al_action: 'D',
        al_added_date: new Date().toISOString().slice(0, 10),
        al_updated_date: null,
        al_status: null,
      }
    );

    // insert for audit permission
    await db.query(
      `INSERT INTO assign_lots SET ?`,
      {
        al_lot_no: lot_no,
        al_user_id: 1,
        al_action: 'A',
        al_added_date: new Date().toISOString().slice(0, 10),
        al_updated_date: null,
        al_status: null,
      }
    );
  } catch (error) {
    throw new Error(`Failed to insert audit: ${error.message}`);
  }
}

async function insertClaim(claimData, quantity, providerId) {
  // console.log("🚀 ~ insertClaim ~ claimData:", claimData)
  try {
    const {memberInfo, dateOfAdmission, dateOfDischarge, dateAdded} = claimData;
    console.log("🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀 ~ insertClaim ~ claimData: 🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀", claimData.claimed);
    console.log("🚀 ~ insertClaim ~ dateAdded:", dateAdded)
    console.log("🚀 ~ insertClaim ~ dateOfDischarge:", dateOfDischarge)
    console.log("🚀 ~ insertClaim ~ dateOfAdmission:", dateOfAdmission)
    let claim_encounter_date = null;
    let claim_discharge_date = null;
    if(claimData?.dateOfAdmission) claim_encounter_date = new Date(claimData?.dateOfAdmission).toISOString().slice(0, 10) || null;
    if(claimData?.dateOfDischarge) claim_discharge_date = new Date(claimData?.dateOfDischarge).toISOString().slice(0, 10) || null;
    // console.log("🚀 ~ insertClaim ~ claim_encounter_date:", claim_encounter_date)
    // console.log("🚀 ~ insertClaim ~ claim_discharge_date:", claim_discharge_date)

    const [result] = await db.query(
      "INSERT INTO claim SET ?",
      {
        claim_number: claimData.max_claim_number,
        claim_auth_id: 0,
        claim_ms_id: memberInfo.ms_id,
        claim_user_id: 1,
        claim_provider_id: claimData.serviceProvider,
        claim_form_no: 0,
        claim_remark: '',
        claim_doc_remark: '',
        claim_prescription_date: claimData.dateOfConsultation,
        claim_auth_req_amt: claimData.claimed,
        claim_discount_amt: 0,
        claim_ded_copay_amt: 0,
        claim_deduction_amt: null,
        claim_net_amt: null,
        claim_medical_report: null,
        claim_dd_medical_report: null,
        claim_anyreporting_doc: null,
        claim_facility_id: 0,
        claim_doctor_id: 0,
        claim_provider_type: 1, // provider type from the provider?
        claim_transaction_date: null,
        claim_provider_remark: '',
        claim_approval_status: claimData.rejected === 0 ? "Approved" : claimData.awarded || claimData.rejected === "PENDING" ? "Pending" : "Denied",
        claim_approval_date: null,
        claim_approval_no: '',
        claim_isDeleted: 0,
        claim_create_date: claimData.dateAdded,
        claim_modify_date: null,
        claim_last_modify_id: null,
        claim_lot_no: claimData.max_lot_no, // do i need to increment this? //increment
        // claim_verified_date: claimData.dateAdded).toISOString().sli,the data in the database use the same date for verified date and create date // give us a default date
        claim_verified_status: 1,
        claim_audit_status: null,
        claim_audit_date: null, // not sure how we get this
        claim_payment_status: 0, // anywhere we have awarded amount greater than zero, we should change status to 1
        claim_payment_date: null, // not sure how we get this
        claim_payment_by_user: null,
        claim_type: claimData.typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2,
        claim_invoice_date: null,
        // claim_is_proforma_generated: 0,
        claim_medical_reviewer: '',
        claim_processed: 1,
        claim_encounter_date: claim_encounter_date, // difference between encounter and prescription date
        claim_discharge_date: claim_discharge_date, // don't know how to get this
        claim_no_stay: 0,
        claim_nn_doctor_id: 0,
        claim_no_session: 0,
        claim_external_dotor: 0,
        claim_send_supervisor: 0,
        claim_total_amount: claimData.claimed,
        recovery_amt: 0,
        claim_master_type: 1,
        claim_network_provider: null,
        claim_nn_provider: null,
        claim_approve_remark: '',
        claim_invoice_number: null,
        claim_file_number: null,
        claim_gross_amt: null,
        claim_before_discount: 0,
        claim_before_copay: 0,
        claim_companion_ttl_stay: null,
        claim_companion_ttl_charge: null,
        claim_vat: 0,
        claim_payable_amt: null,
        claim_materinity_flag: 0,
        claim_emergency: 0,
        claim_reception_date: null,
        claim_discharge_amt: null,
        claim_due_date: null,
        claim_invoice_no: null, // don't know where to get this
        // claim_service_date: claimData.dateOfConsultation,clear
        // claim_external_consumables: 0,
        claim_overall_status: null,
        claim_overall_reason: null,
        claim_approved_amt: null,
        claim_tariff_amt: null, // don't know where to get this
        claim_exceed_amt: 0, // don't know where to get this
        claim_risk_amt: null, //confirm this
        claim_jumbo_amt: 0,
        claim_qty_claimed: quantity.length,
        claim_qty_approved: 0,
        claim_exchange_rate: 1,
        claim_net_amt_lc: null,
        claim_exceed_amt_lc: 0, // don't know where to get this // maybe we can caclulate this using the cost to the requested amount
        claim_appointment_id: null,
        claim_profession: 0,
        claim_profession_fees: 0,
        claim_reimb_provider_state_id: null,
        claim_delivery_charge: null,
        claim_delivery_charge_fee: null,
      }
    );
    return result.insertId;
  } catch (error) {
    throw new Error(`Failed to insert claim: ${error.message}`);
  }
}

async function storeItemsInTables(
  items, 
  drug_service_items,
  {
    claimId, 
    memberInfo, 
    provider, 
    typeOfVisit, 
    diagnosis, 
    networkId
  },
  notFoundItemsFile
) {
    console.log("🚀 ~ items:", items)
    console.log("🚀 ~ drug_service_items:", drug_service_items)
    console.log("🚀 ~ provider:", provider)
    console.log("🚀 ~ networkId:", networkId)
  // i should see 317
  try {
    const labItems = [];
    const drugItems = [];
    const notFoundItems = [];
    const diagnosisItems = [];
    const loggedNotFoundItems = new Set();
    
    const ipOp = typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2;
    const claimType = typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 2 : 1;
    
    if (items.item.length !== items.quantity.length || items.item.length !== items.cost.length) {
      throw new Error("Mismatch in items, quantity, or cost arrays.");
    }
    const LaboratoryItems = drug_service_items.item.services;
    const drugitems = drug_service_items.item.drugs;
    
    // Categorize items
    if(LaboratoryItems.length > 0) {
      for (let [index, item] of LaboratoryItems.entries()) {
      item = item.replace(/^"|"$/g, '').toString(); // Remove quotes from the string
      console.log("🚀 ~ item:", item)
      if (item.startsWith("Entry: ")) {
        console.log("Skipping service....");
        continue; // Skip drug items in the lab loop
      }
      
      const labQuery = `
      SELECT tm_id, tm_tariff_id, tm_net_amt, sl_id, sl_cover_id, tm_cpt_code AS code, tm_description AS description, tm_currency AS currency 
      FROM tariff_master 
      JOIN provider_tariff ON pt_id = tm_tariff_id AND pt_status = '1' 
      JOIN service_list ON sl_id = tm_service_id 
      WHERE tm_cpt_code = '${item}' OR tm_pro_cpt_code = '${item}' OR (tm_pro_description LIKE '%${item}%' OR tm_description = '%${item}%' ) 
      -- AND (pt_networkid = ? OR pt_networkid = 0) 
      -- AND (tm_claim_type = ? OR tm_claim_type = 0) 
      -- AND sl_ip_op != ? 
      LIMIT 1`;
      console.log("🚀 ~ labQuery:", labQuery)
      
      console.log("Checking for lab item:", item);
      const [tariffData] = await db.query(labQuery, [networkId, ipOp, claimType]);
      if(tariffData.length === 0) console.log("No lab data found for item:", item);
      else console.log("Lab data found:", tariffData);

      const tariffQuery = `SELECT pmt_id,pmt_provider_id,pmt_tariff_name,pmt_effective_date,pmt_expiry_date,pmt_added_date,pmt_status
        FROM pbm_provider_medicine_tariff 
        WHERE pmt_provider_id='${provider}' AND pmt_expiry_date is null`;
        let [tariffId] = await db.query(tariffQuery);
        console.log("🚀 ~ tariffId:", tariffId)
        tariffId = tariffId[0]?.pmt_id;
        console.log("🚀 ~ tariffId:", tariffId)
      
      let itemMatched = false;
      
      if (tariffData.length > 0) {
        const itemData = {
          code: tariffData[0].code,
          description: tariffData[0].description,
          cost: items.cost[index],
          quantity: items.quantity[index],
          tm_id: tariffData[0].tm_id, 
          tm_tariff_id: tariffData[0].tm_tariff_id,
          tm_net_amt: tariffData[0].tm_net_amt,
          sl_id: tariffData[0].sl_id,
          sl_cover_id: tariffData[0]?.sl_cover_id,
        }
        // console.log("🚀 ~ itemData:", itemData)
        labItems.push(itemData);
        itemMatched = true;
      }
      // else {
      //   const entry = { item, quantity: items.quantity[index], cost: items.cost[index] };
      //   if (!loggedNotFoundItems.has(entry)) {
      //     console.log("*****************************************1")
      //     notFoundItems.push(entry);
      //     loggedNotFoundItems.add(entry); // Avoid duplicate entries
      //     await logNotFoundItems(notFoundItemsFile, ...notFoundItems);
      //   }
      //   console.warn("Unknown item type:", item);
      // }
        // console.log("🚀 ~ labItems:", labItems)
        if (!itemMatched && items.itemType[index] !== 'Drug' && items.itemType[index] !== 'Drugs') {
          const entry = { item, quantity: items.quantity[index], cost: items.cost[index] };
          const entryKey = JSON.stringify(entry); // unique key
          if (!loggedNotFoundItems.has(entryKey)) {
            notFoundItems.push(entry);
            loggedNotFoundItems.add(entryKey);
          }
        }


      console.log("Items successfully pushed.");
    }
    }

    if(drugItems.length > 0) {
      for (let [index, item] of drugitems.entries()) {
      item = item.replace(/^"|"$/g, ''); // Remove quotes from the string

      if (item.startsWith("Entry: ")) {
        console.log("Skipping drugs....");
        continue; // Skip drug items in the lab loop
      }

      const tariffQuery = `SELECT pmt_id,pmt_provider_id,pmt_tariff_name,pmt_effective_date,pmt_expiry_date,pmt_added_date,pmt_status
        FROM pbm_provider_medicine_tariff 
        WHERE pmt_provider_id='${provider}' AND pmt_expiry_date is null`;
        let [tariffId] = await db.query(tariffQuery);
        console.log("🚀 ~ tariffId:", tariffId)
        tariffId = tariffId[0]?.pmt_id;
        console.log("🚀 ~ tariffId:", tariffId)

      console.log("Fetching Drug")
      const drugQuery = `
      SELECT
       description, md_pro_desc,mu_id,mfo_id,form,chronic_flag,med_code,
        CONCAT_WS(' ',description,'-',trade_name,form,dosage,IF(dispense!='Unit Type',CONCAT_WS(' ',strips,dispense),'')) AS drug_details,med_id,unitform,
        dispense,(CASE WHEN (strips IS NOT NULL OR strips='') THEN 'Strips' ELSE '' END) AS strips
        FROM pbm_medicine
        JOIN pbm_provider_medicine_discount ON md_med_id=med_id AND (md_pro_desc!='' OR md_pro_desc ='')
        JOIN pbm_provider_medicine_tariff ON pmt_id = md_pmt_id AND pmt_status=1
        WHERE med_code = '${item}' OR
        (md_pro_desc like '%${item}%' or 
        (description LIKE '%${item}%' OR trade_name LIKE '%${item}%' OR  form LIKE '%${item}%' OR dosage LIKE '%${item}%' OR unitform LIKE '%${item}%'))
        AND pbm_medicine.status!='0' AND pmt_id = '${tariffId}'
        group by med_id, md_pro_desc, pbm_provider_medicine_discount.md_id 
        LIMIT 1`;
      console.log("🚀 ~ drugQuery:", drugQuery)
      
      const [drugData] = await db.query(drugQuery);

      let drug = drugData;
      console.log("🚀 ~ Drug found successfully:", drugData)
      
      let itemMatched = false;
      
      if (drugData.length === 0) {
        console.warn("No matching drug found for code:", item);
        console.log("Checking for drug_service_items globally...");
        const drugQuery = `
        SELECT md_pro_desc,med_code,
        CONCAT_WS(' ',description,'-',trade_name,form,dosage,IF(dispense!='Unit Type',CONCAT_WS(' ',strips,dispense),'')) AS drug_details,med_id,unitform,
        dispense,(CASE WHEN (strips IS NOT NULL OR strips='') THEN 'Strips' ELSE '' END) AS strips, pud_id, md_provider_id
        FROM auth_drugs_prescribed
        INNER JOIN pbm_medicine ON adp_drug_code=med_code 
        INNER JOIN pbm_unit_details ON adp_dosage_unit=pud_id
        JOIN pbm_provider_medicine_discount md ON md_med_id=med_id AND (md_pro_desc!='' OR md_pro_desc ='')
        WHERE med_code = '%${item}%' OR (md_pro_desc LIKE '%${item}%' OR description LIKE '%${item}%') AND pbm_medicine.status!='0'`;
        
        const [drugData] = await db.query(drugQuery, [item, item]);
        drug = drugData;
        console.log("Drug data found for drug item globally:", drug);
      }
      
       if (drug.length > 0) {
        const itemData = {
          code: drug[0].med_code,
          description: drug[0].drug_details || drug[0].md_pro_desc,
          cost: items.cost[index],
          quantity: items.quantity[index],
          md_pro_desc: drug[0].md_pro_desc, 
          med_code: drug[0].med_code,
          med_id: drug[0].med_id, 
          unitform: drug[0].unitform,
          pud_id: drug[0].pud_id
        }
        drugItems.push(itemData);
        itemMatched = true;
      } 
      
      // else {
      //   const entry = { item, quantity: items.quantity[index], cost: items.cost[index] };
      //   if (!loggedNotFoundItems.has(entry)) {
      //     console.log("*****************************************1")
      //     notFoundItems.push(entry);
      //     loggedNotFoundItems.add(entry); // Avoid duplicate entries
      //     await logNotFoundItems(notFoundItemsFile, ...notFoundItems);
      //   }
      //   console.warn("Unknown item type:", item);
      // }
        // console.log("🚀 ~ labItems:", labItems)

        if (!itemMatched) {
        const entry = { item, quantity: items.quantity[index], cost: items.cost[index] };
        const entryKey = JSON.stringify(entry); // unique key
        if (!loggedNotFoundItems.has(entryKey)) {
          notFoundItems.push(entry);
          loggedNotFoundItems.add(entryKey);
        }
      }

      console.log("Items successfully pushed.");
    }
    }
    
    // console.log("🚀 ~+++++++++++++++++++++++++++++++ diagnosis:", diagnosis);
    
    if(typeof diagnosis === "string") {
      // console.log("🚀 ~****************************** insertDiagnosis ~ diagnosis:", diagnosis)
      diagnosis = [diagnosis];
      // console.log("🚀 ~******************************** insertDiagnosis ~ diagnosis:", diagnosis)
    };
    // console.log("🚀 ~+++++++++++++++++++++++++++++++++++++ diagnosis:", diagnosis);
    for(let d of diagnosis) {
      d = d.replace(/^"|"$/g, ''); // Remove quotes from the string
      console.log("Checking for diagnosis:", d);
      const sql = `
      SELECT icd_id,icd_code, icd_long_description, icd_cover_id 
      FROM icd_detail_cm 
      WHERE (icd_short_description LIKE '%${d}%' OR icd_long_description LIKE '%${d}%')
      LIMIT 1`;
      const [diagnosisCode] = await db.query(sql);
      console.log("🚀 ~ diagnosisCode:", diagnosisCode)

      if(diagnosisCode.length > 0) {
        diagnosisItems.push({
          code: diagnosisCode[0].icd_code,
          description: diagnosisCode[0].icd_long_description
        });
      } else {
        console.warn("No matching diagnosis code found for:", d);
        const entry = { diagnosis: d };
        const entryKey = JSON.stringify(entry); // unique key
        // console.log("🚀 ~ entry:", entry)
        if (!loggedNotFoundItems.has(entryKey)) {
          console.log("Doesn't have entry")
          notFoundItems.push(entry);
          console.log("push entry")
          loggedNotFoundItems.add(entryKey); // Avoid duplicate entries
          console.log("**********************************************2")
          // console.log("🚀 ~ notFoundItems:", notFoundItems)
          // console.log("🚀 ~ notFoundItems:", {...notFoundItems})
          // console.log("🚀 ~ notFoundItemsFile:", notFoundItemsFile)
          console.log("log entry")
          continue;
        }
      }
    }
    
    await insertLabItems(labItems, claimId);
    await insertDrugItems(drugItems, claimId, memberInfo);
    await insertDiagnosis(diagnosisItems, claimId);

    if(notFoundItems.length > 0) {
      console.log("Logging not found items...");
      await logNotFoundItems(notFoundItemsFile, notFoundItems);
    }
  } catch (error) {
    throw new Error(`Failed to store items: ${error}`);
  }
}

async function insertLabItems(labItems, claimId) {
  // console.log("🚀 ~ insertLabItems ~ labItems:", labItems)
  try {
    for (const labItem of labItems) {
        await db.query("INSERT INTO claim_details SET ?", {
        cd_claim_id: claimId,
        cd_auth_id: 0,
        cd_cover_id: labItem.sl_cover_id,
        cd_plan_id: 0,
        cd_activity_type: labItem.sl_id,
        cd_tooth: '',
        cd_activity_code: labItem.code,
        cd_activity_des: labItem.description,
        cd_activity_req_amt: labItem.cost,
        cd_discount_amt: 0,
        cd_ded_copay_amt: 0,
        cd_deduction_amt: labItem.cost - labItem.cost,
        cd_qty_claimed: labItem.quantity,
        // cd_qty_approved: labItem.quantity, // are all of them approved?
        cd_qty_approved: null, // are all of them approved?
        cd_claimed_amt: labItem.cost, // how do we know the claimed amt
        // cd_approved_amt: labItem.cost, // how do we know the approved amt
        cd_approved_amt: null, // how do we know the approved amt
        cd_recovery: 0,
        cd_activity_net_amt: labItem.tm_net_amt,
        cd_activity_remark: '',
        cd_eye: null,
        cd_cylinder: null,
        cd_spine: null,
        cd_access: null,
        cd_pack_price: null,
        cd_no_session: null,
        // cd_auth_approval: labItem.cost - labItem.cost === 0 ? 1 : 0,
        cd_auth_approval: null,
        cd_stay: null,
        cd_risk_net_amt: labItem.tm_net_amt,
        cd_is_skip: 0,
        cd_den_code: null,
        cd_tarif_id: labItem.tm_tariff_id,
        cd_exceed_amt: 0,
        cd_vat_amt: 0,
        cd_before_copay: 0,
        cd_before_discount: 0,
        cd_tm_id: labItem.tm_id,
        cd_pkg_code: null,
        cd_payable_amt: labItem.cost,
        cd_uncovered_amt: 0,
        cd_perday_amt: 0,
        cd_is_nonnetwork: 0,
        cd_doctor_id: 0,
        // cd_service_flag: 0,
        cd_activity_req_qty: null,
        cd_pkg_description: null,
        cd_pkg_icu: null,
        cd_pkg_ward: null,
        cd_prov_net_amt: null,
        cd_master_type: 1,
        cd_den_code_des: null,
        cd_add_den_code_desc: null,
        cd_service_date: null,
        cd_activity_net_qty: null,
        cd_net_amt_paid: null,
        cd_extra_flag: null,
        cd_tariff_amt: labItem.tm_net_amt,
        cd_currency: labItem.currency,
        cd_exchange_rate: 1,
      });

      console.log(`Lab item successfully inserted - code: ${labItem.code} desc: ${labItem.description}, claimId: ${claimId}`);
    }
  } catch (error) {
    console.error("Error inserting lab items:", error.message);
  }
}

async function insertDrugItems(drugItems, claimId, memberInfo) {
  try {
    for (const drugItem of drugItems) {
      const { md_pro_desc, med_code, med_id, unitform, pud_id } = drugItem;
        await db.query("INSERT INTO claim_drugs_prescribed SET ?", {
          cdp_claim_id: claimId, 
          cdp_auth_id: 0,
          cdp_cover_id: '112',
          cdp_ms_id: memberInfo.ms_id,
          cdp_pro_desc: md_pro_desc === "" ? drug_details : md_pro_desc,
          cdp_drug_code: med_code,
          // cdp_dosage_per_tme: '', //??
          cdp_dosage_unit: pud_id,
          cdp_dosage_frequency: null, //??
          cdp_prescribed_by: 'Day', //??
          cdp_prescribed_period: null, //??
          cdp_tot_dosage_prescribed: drugItem.quantity, //??
          cdp_tot_dosage_prescribed_unit: unitform,
          cdp_dispense_unit: drugItem.quantity, //??
          cdp_dispense_type: unitform,
          cdp_dispense_unit1: 0, //??
          cdp_dispense_type1: null, //??
          cdp_gross_amount: drugItem.cost, //??
          cdp_discount: 0,
          cdp_copay: 0,
          cdp_net_price: drugItem.cost, //??
          cdp_net_price_bill: drugItem.cost, //??
          cdp_remark: '',
          cdp_status: null,
          // cdp_status: '1',
          cdp_created_date: new Date().toISOString().slice(0, 10), // since no date was provided, using current date
          cdp_modify_date: null,
          cdp_extra_dispense: `0 ${drugItem.unitform}`,
          cdp_isdispense: 0,
          cdp_drug_dispenseby: 0,
          cdp_dispense_date: null,
          // cdp_drug_status: 'Approved', // we have denial reason, but no way to know which is denied
          cdp_drug_status: 'Pending', // we have denial reason, but no way to know which is denied
          cdp_is_skip: 0,
          cdp_den_code: null,
          cdp_den_code_des: null,
          cdp_add_den_code_desc: null,
          cdp_approve_amt: null,
          // cdp_approve_amt: drugItem.cost,
          cdp_changed_price: 0,
          cdp_qty_claimed_val: 1, // confirm
          cdp_qty_claimed_unit: unitform,
          cdp_qty_approved_val: null, // confirm
          cdp_qty_approved_unit: null,
          // cdp_qty_approved_val: 1, // confirm
          // cdp_qty_approved_unit: unitform,
          cdp_final_price: drugItem.cost,
          cdp_drug_price: drugItem.cost,
          cdp_deduction_amt: 0,
          cdp_exceeding_limitation: 0,
          cdp_debited_amt: 0,
          cdp_risk_recovery_amt: 0,
          cdp_risk_carrier_net_amt: drugItem.cost,
          cdp_exgratia: 0,
        });

      console.log(`Drug item successfully inserted - code: ${med_code} desc: ${drugItem.description}, claimId: ${claimId}`);
    }
  } catch (error) {
    console.error("Error inserting drug items:", error.message);
  }
}

async function insertDiagnosis(diagnosisItems, claimId) {
  try {

    for(let diagnosis of diagnosisItems) {   
        await db.query("INSERT INTO claim_codes SET ?", {
          cc_claim_id: claimId,
          cc_auth_id: 0,
          cc_code: diagnosis.code,
          cc_code_des: diagnosis.description,
          cc_code_etc: null,
          cc_code_type: 'ICD',
          cc_status: 'Pending', // how do we know which is approved
          // cc_status: 'Approved', // how do we know which is approved
          cc_remark: '',
          cc_is_skip: 0,
          cc_den_code: null,
          cc_den_code_des: null,
          cc_add_den_code_desc: null,
        });
  
      console.log(`Diagnosis successfully inserted - code: ${diagnosis.code} desc: ${diagnosis.description}, claimId: ${claimId}`);
    }
  } catch (error) {
    console.error("Error inserting diagnosis:", error.message);
  }
}

// async function logNotFoundItems(filePath, items) {
//   // console.log("🚀 ~ logNotFoundItems ~ items:", items)
//   try {
//     const logEntry = JSON.parse(items);
//     await fsp.appendFile(filePath, logEntry, "utf8");
//     console.log("Logged not-found items to file.");
//   } catch (error) {
//     console.error("Error logging not-found items:", error.message);
//   }
// }

async function logNotFoundItems(filePath, data) {
  try {
    const existingData = await fsp.readFile(filePath, 'utf-8');
    const records = existingData ? JSON.parse(existingData) : [];
    if (!Array.isArray(records)) {
      throw new Error('Invalid data format: expected an array');
    }
    records.push(...data);
    await fsp.writeFile(filePath, JSON.stringify(records, null, 2), 'utf-8');
  } catch (error) {
    console.error('Error logging failed record:', error);
  }
}

async function getProviderNetworkId(providerId, ms_plan_network) {
    console.log("🚀 ~ getProviderNetworkId ~ ms_plan_network:", ms_plan_network)
    const providerNetworkQuery = `
      SELECT pt_networkid, pt_id 
      FROM provider_tariff 
      WHERE pt_status = '1' AND pt_provider_id = ?`;
    const [network] = await db.query(providerNetworkQuery, [providerId]);
    console.log("🚀 ~ getProviderNetworkId ~ network:", network)

    console.log("🚀 ~ getProviderNetworkId ~ network[0]?.pt_networkid:", network[0]?.pt_networkid)
    const networkId = network[0]?.pt_networkid != (0 || undefined)
    ? network[0]?.pt_networkid
    : ms_plan_network != 0
    ? ms_plan_network
    : 0;
    console.log("🚀 ~ getProviderNetworkId ~ networkId:", networkId)

    return {networkId};
}

async function logFailedRecord(filePath, data) {
  console.log("🚀 ~ logFailedRecord ~ filePath:", filePath)
  try {
    const existingData = await fsp.readFile(filePath, 'utf-8');
    console.log("🚀 ~ logFailedRecord ~ existingData:", existingData)
    const records = existingData ? JSON.parse(existingData) : [];
    if (!Array.isArray(records)) {
      throw new Error('Invalid data format: expected an array');
    }
    records.push(data);
    await fsp.writeFile(filePath, JSON.stringify(records, null, 2), 'utf-8');
  } catch (error) {
    console.error('Error logging failed record:', error);
  }
}

function splitName(employee) {
  const [mm_name, ...rest] = employee.trim().split(/\s+/);
  const family_name = rest.pop() || "";
  const fathers_name = rest.join(" ");
  return { mm_name, fathers_name, family_name };
}

function formatDuration(duration) {
  const milliseconds = parseInt((duration % 1000) / 100);
  const seconds = Math.floor((duration / 1000) % 60);
  const minutes = Math.floor((duration / (1000 * 60)) % 60);
  const hours = Math.floor((duration / (1000 * 60 * 60)) % 24);
  return `${hours}h ${minutes}m ${seconds}s ${milliseconds}ms`;
}

async function findMemberInfo(memberNumber) {
  try {
    memberNumber = String(memberNumber)
    const [member] = await db.query(
      `
      SELECT ms_id, ms_tob_plan_id, ms_plan_network, mm_member_id, mm_member_id
      FROM members
      INNER JOIN members_schemes ms ON ms.ms_member_id = members.mm_id
      WHERE (mm_nin_number = ? OR mm_national_id = ? OR mm_member_id = ?) 
      ORDER BY ms_id DESC LIMIT 1`,
      [memberNumber, memberNumber, memberNumber]
    );
    console.log("🚀 ~ findMemberInfo ~ member:", member)
    if (!member || !member.length) {
      throw new Error(`No member found for memberNumber: ${memberNumber}`);
    }
    return member[0];
  } catch (error) {
    console.log(`Failed to fetch member info: ${error.message}`);
  }
}

async function findLastClaimAndLotNumber() {
  try {
    const [claim] = await db.query(
      `
      SELECT MAX(CAST(claim_number AS UNSIGNED)) AS max_claim_number
      FROM claim
      WHERE claim_number REGEXP '^[0-9]+$'`
    );
    return {
      max_claim_number: claim?.[0]?.max_claim_number + 1 || 300000000,
    };
  } catch (error) {
    throw new Error(`Failed to fetch last claim number: ${error.message}`);
  }
}

async function getProviderInfo(providerId) {  
  // console.log("🚀 ~ getProviderInfo ~ providerId:", providerId)
  const providerQuery = `
  SELECT provider_id, provider_name 
  FROM providers 
  WHERE provider_name LIKE '%${providerId}%' OR provider_id = '${providerId}';
  `;
  // console.log("🚀 ~ getProviderInfo ~ providerQuery:", providerQuery)

  // const likeValue = `%${providerId}%`; // Add wildcards around the providerId
  const [provider] = await db.query(providerQuery);

  console.log("🚀 ~ getProviderInfo ~ provider:", provider);
  return provider;
}

async function maxLotNumber() {
  try {
    const [lot] = await db.query(
      `
      SELECT MAX(lot_no) AS max_lot_no
      FROM lots
      WHERE lot_master_type = 1`
    );
    return {
      max_lot_no: lot?.[0]?.max_lot_no + 1 || 100000000,
    };
  }
  catch (error) {}
}

async function checkAndSegregateFailedRecords(failedRecordsFile) {
  try {
    const data = await fsp.readFile(failedRecordsFile, 'utf-8');
    const records = JSON.parse(data);
    if (records.length > 0) {
      console.log(`Found ${records.length} failed records to segregate.`);
      await segregateFailedRecords(failedRecordsFile);
    } else {
      console.log('No failed records to segregate.');
    }
  } catch (error) {
    console.error('Error checking failed records:', error);
  }
}

async function logSuccessfulRecord(filePath, record, claim_number) {
  // console.log("🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀 ~ logSuccessfulRecord ~ claim_number:", claim_number)
  try {
    console.log('Attempting to log successful record:', record);
    const existingData = await fsp.readFile(filePath, 'utf-8');
    const records = existingData ? JSON.parse(existingData) : [];
    if (!Array.isArray(records)) {
      throw new Error('Invalid data format: expected an array');
    }
    const recordWithClaimId = { ...record, claim_number };
    records.push(recordWithClaimId);
    await fsp.writeFile(filePath, JSON.stringify(records, null, 2), 'utf-8');
    console.log('Successfully logged record to:', filePath);
  } catch (error) {
    console.error('Error logging successful record:', error);
  }
}

// migrateClaimGen().catch((error) => {
//   console.error("Critical error in migration:", error);
//   process.exit(1);
// });

module.exports = { migrateClaimGen };
