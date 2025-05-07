const db = require("../db");
const fs = require("fs").promises;
const path = require("path");
const { segregateFailedRecords } = require("./segregateFailedRecords");
const { logFailedRecord } = require("./failed-record.logger");

const processedClaims = new Set(); // For `failed_records.json`
const processedItems = new Set();
const loggedNotFoundItems = new Set(); // For `not_found_items.json`
const processedFiles = new Set();
async function migrateClaimDec(
  jsonPath,
  successfulRecordsFile,
  failedRecordsFile,
  notFoundItemsFile
) {
  const startTime = new Date();
  // const filePath = path.resolve(__dirname, "./output/successes.json");
  const filePath = path.resolve(__dirname, "./output/output.json");
  const batchSize = parseInt(process.env.BATCH_SIZE) || 20;
  console.log(`Configured batch size: ${batchSize}`);
  const stats = {
    totalRecords: 0,
    processedRecords: 0,
    failedRecords: 0,
    skippedRecords: 0,
  };
  try {
    if (processedFiles.has(jsonPath)) {
      console.warn(`⚠️ Duplicate execution detected! Skipping ${jsonPath}`);
      return;
    }
    processedFiles.add(jsonPath);

    const jsonData = await readJsonFile(jsonPath);
    // const jsonData = jsonPath;
    stats.totalRecords = jsonData.length;
    console.log(`Total records to process: ${stats.totalRecords}`);

    const batches = createBatches(jsonData, batchSize);
    for (const batch of batches) {
      const batchResults = await processBatch(
        batch,
        failedRecordsFile,
        successfulRecordsFile,
        notFoundItemsFile
      );
      stats.processedRecords += batchResults.processed;
      stats.failedRecords += batchResults.failed;
      stats.skippedRecords += batchResults.skipped;
      const sth2 = await db.query("SELECT * FROM lots WHERE lot_no = ?", [
        "110001948",
      ]);
      console.log("🚀 ~ after everything:", sth2[0]);
    }

    // Log comprehensive migration summary
    console.log(
      JSON.stringify(
        {
          startTime,
          endTime: new Date(),
          duration: formatDuration(new Date() - startTime),
          stats,
        },
        null,
        2
      )
    );

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

async function processBatch(
  batch,
  failedRecordsFile,
  successfulRecordsFile,
  notFoundItemsFile
) {
  const batchResults = {
    processed: 0,
    failed: 0,
    skipped: 0,
  };
  for (const record of batch) {
    try {
      const { claimId, claim_number } = await processRecord(
        record,
        failedRecordsFile,
        successfulRecordsFile,
        notFoundItemsFile
      );
      const sth2 = await db.query("SELECT * FROM lots WHERE lot_no = ?", [
        "110001948",
      ]);
      console.log("🚀 ~ after everything:", sth2[0]);
      batchResults.processed++;
      await logSuccessfulRecord(successfulRecordsFile, record, claim_number);
    } catch (error) {
      console.error(`Error processing record: ${error.message}`);
      await logFailedRecord(failedRecordsFile, {
        record,
        error: error.message,
        stackTrace: error.stack,
      });
      batchResults.failed++;
    }
  }
  return batchResults;
}

async function readJsonFile(filePath) {
  const jsonCache = new Map();
  try {
    if (jsonCache.has(filePath)) {
      console.log(`🔄 Using cached JSON for: ${filePath}`);
      return jsonCache.get(filePath);
    }
    console.log(`📂 Reading JSON file: ${filePath}`);
    const data = await fs.readFile(filePath, "utf8");
    const jsonData = JSON.parse(data);
    jsonCache.set(filePath, jsonData);
    return jsonData;
  } catch (error) {
    throw new Error(`Failed to read JSON file: ${error.message}`);
  }
}

async function processRecord(
  record,
  failedRecordsFile,
  successfulRecordsFile,
  notFoundItemsFile
) {
  let connection;
  try {
    record.dateOfConsultation = addOneDay(record.dateOfConsultation);
    console.log("🚀 ~ record.dateOfConsultation:", record.dateOfConsultation);
    record.dateOfAdmission = addOneDay(record.dateOfAdmission);
    console.log("🚀 ~ record.dateOfAdmission:", record.dateOfAdmission);
    record.dateOfDischarge = addOneDay(record.dateOfDischarge);
    console.log("🚀 ~ record.dateOfDischarge:", record.dateOfDischarge);

    connection = await db.getConnection();
    await connection.beginTransaction();
    const claimRemark = "Migrated";
    record.typeOfVisit = normalizeTypeOfVisit(record.typeOfVisit);

    const memberInfo = await findMemberInfo(
      connection,
      record.memberNumber,
      record.memberNumber1
    );
    if (!memberInfo) {
      throw new Error(
        `Member not found for memberNumber: ${record.memberNumber}`
      );
    }

    console.log("Processing record for member:", memberInfo.mm_member_id);

    const provider = await getProviderInfo(connection, record.serviceProvider);
    const providerId = provider[0]?.provider_id;
    const providerType = provider[0]?.provider_type;
    if (!providerId) {
      throw new Error(
        `Provider not found for serviceProvider: ${record.serviceProvider}`
      );
    }

    const { max_claim_number } = await findLastClaimAndLotNumber(connection);
    let max_lot_no;

    try {
      max_lot_no = await getOrCreateLotStatement(
        connection,
        memberInfo,
        record,
        claimRemark
      );
    } catch (error) {
      console.error("Error in createLotStatement:", error);
      throw error;
    }

    try {
      await insertAuditAndDataEntry(connection, max_lot_no);
    } catch (error) {
      console.error("Error in insertAuditAndDataEntry:", error);
      throw error;
    }

    let claimId = 0;
    try {
      claimId = await insertClaim(
        connection,
        {
          max_claim_number,
          max_lot_no,
          memberInfo,
          serviceProvider: providerId,
          dateOfConsultation: record.dateOfConsultation,
          claimed: record.claimed,
          awarded: record.awarded,
          dateAdded: record.dateAdded,
          typeOfVisit: record.typeOfVisit,
          dateOfAdmission: record.dateOfAdmission,
          dateOfDischarge: record.dateOfDischarge,
          // items 👇
          item: record.item,
          itemType: record.itemType,
          mapped: record.mapped,
          quantity: record.quantity,
          quantityApproved: record.quantityApproved,
          rejectionReason: record.rejectionReasons,
          rejected: record.rejected,
          awarded: record.awarded,
          cost: record.cost,
          total: record.total,
        },
        record.quantity,
        providerId,
        providerType,
        claimRemark
      );
    } catch (error) {
      console.error("Error in insertClaim:", error);
      throw error;
    }

    console.log(
      `Claim inserted with ID: ${+claimId} and claim number: ${+max_claim_number}`
    );

    const { ms_plan_network } = memberInfo;
    const { networkId } =
      (await getProviderNetworkId(
        connection,
        record.serviceProvider,
        ms_plan_network
      )) || 0;

    try {
      await storeItemsInTables(
        connection,
        {
          item: record.item,
          itemType: record.itemType,
          mapped: record.mapped,
          quantity: record.quantity,
          quantityApproved: record.quantityApproved,
          rejectionReason: record.rejectionReasons,
          rejected: record.rejected,
          awarded: record.awarded,
          cost: record.cost,
          total: record.total,
        },
        {
          claimId,
          memberInfo,
          provider: providerId,
          typeOfVisit: record.typeOfVisit,
          dateOfConsultation: record.dateOfConsultation,
          diagnosis: record.diagnosis,
          networkId,
        },
        notFoundItemsFile
      );

      console.log(
        "+++++++++++++++++======================+++++++++++++++++++++++++"
      );
      await updateClaimTotals(connection, claimId, record, max_lot_no);
    } catch (error) {
      console.error("Error in storeItems in table", error);
      throw error;
    }

    // Commit transaction if all steps succeed
    await connection.commit();
    const sth2 = await connection.query("SELECT * FROM lots WHERE lot_no = ?", [
      max_lot_no,
    ]);
    console.log("🚀 ~ after everything:", sth2[0]);

    return { claimId, claim_number: max_claim_number };
  } catch (error) {
    console.error("Error in processRecord → will roll back:", error);
    console.error("Error in processRecord:", {
      message: error.message,
      stack: error.stack,
      record: record,
    });

    if (connection) {
      const sth2 = await connection.query(
        "SELECT * FROM lots WHERE lot_no = ?",
        ["110001948"]
      );
      console.log("🚀 ~ after everything:", sth2[0]);
      await connection.rollback();
      console.log("Transaction rolled back successfully");
      throw error;
    }
  } finally {
    if (connection) {
      connection.release();
    }
  }
}

async function updateClaimTotals(connection, claimId, record, lot) {
  const {
    memberInfo,
    dateOfAdmission,
    dateOfDischarge,
    rejectionReason,
    claimed,
    dateOfConsultation,
    typeOfVisit,
  } = record;
  console.log("🚀 ~ updateClaimTotals ~ record:", record);
  console.log("🚀 ~ updateClaimTotals ~ typeOfVisit:", typeOfVisit);
  console.log("🚀 ~ updateClaimTotals ~ dateOfAdmission:", dateOfAdmission);
  console.log(
    "🚀 ~ updateClaimTotals ~ dateOfConsultation:",
    dateOfConsultation
  );
  console.log("🚀 ~ updateClaimTotals ~ dateOfDischarge:", dateOfDischarge);
  let claim_encounter_date = null;
  let claim_discharge_date = null;
  let claim_consultation_date = null;
  let noOfStays = 0;
  if (dateOfConsultation)
    claim_consultation_date =
      new Date(dateOfConsultation).toISOString().slice(0, 10) || null;
  if (dateOfAdmission)
    claim_encounter_date =
      new Date(dateOfAdmission).toISOString().slice(0, 10) || null;
  console.log(
    "🚀 ~ updateClaimTotals ~ claim_encounter_date:",
    claim_encounter_date
  );
  if (dateOfDischarge)
    claim_discharge_date =
      new Date(dateOfDischarge).toISOString().slice(0, 10) || null;
  console.log(
    "🚀 ~ updateClaimTotals ~ claim_discharge_date:",
    claim_discharge_date
  );

  if (claim_encounter_date && claim_discharge_date) {
    noOfStays = calculateHospitalStay(
      claim_encounter_date,
      claim_discharge_date
    );
  }
  console.log("🚀 ~ updateClaimTotals ~ claimId:", claimId);
  const [serviceDetails] = await connection.query(
    `SELECT SUM(cd_activity_req_amt) AS total_claimed, 
      SUM(cd_approved_amt) AS total_approved, 
      SUM(cd_deduction_amt) AS total_deduction,
      SUM(cd_tariff_amt) AS total_tariff 
      FROM claim_details WHERE cd_claim_id = (SELECT claim_id FROM claim WHERE claim_id = ?)`,
    [claimId]
  );
  console.log("🚀 ~ updateClaimTotals ~ serviceDetails:", serviceDetails);

  const [drugDetails] = await connection.query(
    `SELECT SUM(cdp_gross_amount) AS total_claimed, 
      SUM(cdp_approve_amt) AS total_approved, 
      SUM(cdp_deduction_amt) AS total_deduction,
      SUM(cdp_drug_price) as total_tariff
      FROM claim_drugs_prescribed WHERE cdp_claim_id = (SELECT claim_id FROM claim WHERE claim_id = ?)`,
    [claimId]
  );
  console.log("🚀 ~ updateClaimTotals ~ drugDetails:", drugDetails);

  const totalClaimed =
    (+serviceDetails[0].total_claimed || 0) +
    (+drugDetails[0].total_claimed || 0);
  console.log("🚀 ~ updateClaimTotals ~ totalClaimed:", totalClaimed);
  const totalApproved =
    (+serviceDetails[0].total_approved || 0) +
    (+drugDetails[0].total_approved || 0);
  console.log("🚀 ~ updateClaimTotals ~ totalApproved:", totalApproved);
  const totalDeducted =
    (+serviceDetails[0].total_deduction || 0) +
    (+drugDetails[0].total_deduction || +totalClaimed - +totalApproved);
  console.log("🚀 ~ updateClaimTotals ~ totalDeducted:", totalDeducted);
  const totalTariffAmt =
    (+serviceDetails[0].total_tariff || 0) +
    (+drugDetails[0].total_tariff || 0);
  console.log("🚀 ~ updateClaimTotals ~ totalTariffAmt:", totalTariffAmt);

  await connection.query(`UPDATE claim SET ? WHERE claim_id = ?`, [
    {
      claim_type: typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2,
      claim_service_date: record.dateOfConsultation,
      claim_prescription_date: claim_consultation_date,
      claim_encounter_date: claim_encounter_date,
      claim_discharge_date: claim_discharge_date,
      claim_service_date: claim_consultation_date,
      claim_net_amt: totalApproved.toFixed(2),
      claim_no_stay: noOfStays,
      claim_tariff_amt: totalTariffAmt,
      claim_approved_amt: totalApproved.toFixed(2),
      claim_risk_amt: totalApproved.toFixed(2),
      claim_auth_req_amt: totalClaimed.toFixed(2),
      claim_no_session: totalClaimed.toFixed(2),
      claim_total_amount: totalClaimed.toFixed(2),
      claim_payable_amt: totalClaimed.toFixed(2),
      claim_deduction_amt: totalDeducted.toFixed(2),
      claim_lot_no: lot,
    },
    claimId,
  ]);
}

const lotCache = new Map();
async function getOrCreateLotStatement(
  connection,
  memberInfo,
  record,
  claimRemark
) {
  //   console.log("🚀 ~ getOrCreateLotStatement ~ record:", record);

  const { max_lot_no } = await maxLotNumber(connection);
  console.log("🚀 ~ getOrCreateLotStatement ~ max_lot_no:", max_lot_no);

  // Use a key combining service provider and dateOfConsultation.
  const dateKey = record.dateOfConsultation.slice(0, 7); // e.g., "2024-12"
  const key = `${record.serviceProvider}|${dateKey}`;

  // Determine the record's type (1 for IN-PATIENT, 2 for OUT-PATIENT)
  console.log("🚀 ~ record.typeOfVisit:", record.typeOfVisit);
  const recordType = ["OUT-PATIENT", "OutPatient"].includes(record.typeOfVisit)
    ? 1 // out‑patient
    : 2;
  console.log("🚀 ~ recordType:", recordType);

  console.log("🚀 ~ getOrCreateLotStatement ~ key:", key);
  if (lotCache.has(key)) {
    console.log(`Reusing lot for ${key}: ${lotCache.get(key)}`);

    await connection.query(
      "UPDATE lots SET lot_total_claim = IFNULL(lot_total_claim, 0) + 1 WHERE lot_no = ?",
      [max_lot_no]
    );
    return lotCache.get(key);
  }

  const sth = await connection.query("SELECT * FROM lots WHERE lot_no = ?", [
    max_lot_no,
  ]);
  console.log("🚀 ~ before create lot statement function:", sth[0]);

  // Assume maxLotNumber() returns an object with property max_lot_no.
  await createLotStatement(
    connection,
    memberInfo,
    max_lot_no,
    record,
    claimRemark
  );
  lotCache.set(key, max_lot_no);
  console.log(`Created new lot for ${key}: ${max_lot_no}`);

  const sth2 = await connection.query("SELECT * FROM lots WHERE lot_no = ?", [
    max_lot_no,
  ]);
  console.log("🚀 ~ after create lot statement function:", sth2[0]);

  await connection.query(
    "UPDATE lots SET lot_type = 0 WHERE lot_no = ? AND lot_type <> ? AND lot_type <> 0",
    [max_lot_no, recordType]
  );

  return max_lot_no;
}

async function createLotStatement(
  connection,
  memberInfo,
  lot_no,
  record,
  claimRemark
) {
  console.log("🚀 ~ claimRemark:", claimRemark);
  // create new statement
  console.log("Creating new statement...");

  const provider = await getProviderInfo(connection, record.serviceProvider);
  try {
    const result = await connection.query("INSERT INTO lots SET ?", {
      lot_no: lot_no,
      lot_receive_date: new Date(record.dateOfConsultation)
        .toISOString()
        .slice(0, 10),
      lot_from_date: null,
      lot_to_date: null,
      lot_type: ["OUT-PATIENT", "OutPatient"].includes(record.typeOfVisit)
        ? 1
        : 2, // in patient or out patient
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
      lot_source: "offline",
      lot_ic_id: null,
      // lot_deleted:0,
      lot_modified_date: null,
      lot_audit_close_date: new Date(record.dateOfConsultation)
        .toISOString()
        .slice(0, 10),
      lot_claim_close_date: null,
      lot_master_type: 1,
      lot_grp: memberInfo.ms_policy_id,
      lot_t: "G",
      lot_remark: claimRemark,
      lot_is_chronic: 0,
      lot_management_status: 0,
      lot_management_user_id: 0,
      lot_closed_user_id: 0,
      lot_merged_user_id: 0,
    });
    console.log(
      "****************************************************************"
    );
    console.log("🚀 ~ result:", result);

    const sth = await connection.query("SELECT * FROM lots WHERE lot_no = ?", [
      lot_no,
    ]);
    console.log("🚀 ~ in create lot statement function:", sth[0]);
  } catch (error) {
    throw new Error(`Failed to insert lot: ${error}`);
  }
}

async function insertAuditAndDataEntry(connection, lot_no) {
  try {
    // insert for data entry permission
    await connection.query(`INSERT INTO assign_lots SET ?`, {
      al_lot_no: lot_no,
      al_user_id: 1,
      al_action: "D",
      al_added_date: new Date().toISOString().slice(0, 10),
      al_updated_date: null,
      al_status: null,
    });

    // insert for audit permission
    await connection.query(`INSERT INTO assign_lots SET ?`, {
      al_lot_no: lot_no,
      al_user_id: 1,
      al_action: "A",
      al_added_date: new Date().toISOString().slice(0, 10),
      al_updated_date: null,
      al_status: null,
    });
  } catch (error) {
    throw new Error(`Failed to insert audit: ${error.message}`);
  }
}

function normalizeTypeOfVisit(type) {
  const mapping = {
    "IN PATIENT": "IN-PATIENT",
    "OUT PATIENT": "OUT-PATIENT",
    OUTPATIENT: "OUT-PATIENT",
    INPATIENT: "IN-PATIENT",
    "OUT- PATIENT": "OUT-PATIENT",
    "IN -PATIENT": "IN-PATIENT",
    "OUT -PATIENT": "OUT-PATIENT",
    "IN- PATIENT": "IN-PATIENT",
    "IN-_X0006_PATIENT": "IN-PATIENT",
    IN_x0006_PATIENT: "IN-PATIENT",
    "OUT-_X0006_PATIENT": "OUT-PATIENT",
    OUT_x0006_PATIENT: "OUT-PATIENT",
  };
  return mapping[type] || type;
}

async function insertClaim(
  connection,
  claimData,
  quantity,
  providerId,
  providerType,
  claimRemark
) {
  try {
    const {
      memberInfo,
      dateOfAdmission,
      dateOfDischarge,
      rejectionReason,
      claimed,
      dateOfConsultation,
    } = claimData;
    const nonApproved = rejectionReason.filter(
      (reason) => reason !== "Approved"
    ).length;

    let claim_encounter_date = null;
    let claim_discharge_date = null;
    let claim_consultation_date = null;
    if (dateOfConsultation)
      claim_consultation_date =
        new Date(claimData?.dateOfConsultation).toISOString().slice(0, 10) ||
        null;
    if (dateOfAdmission)
      claim_encounter_date =
        new Date(claimData?.dateOfAdmission).toISOString().slice(0, 10) || null;
    if (dateOfDischarge)
      claim_discharge_date =
        new Date(claimData?.dateOfDischarge).toISOString().slice(0, 10) || null;
    const numberOfStays = calculateHospitalStay(
      claim_encounter_date,
      claim_discharge_date
    );

    const [result] = await connection.query("INSERT INTO claim SET ?", {
      claim_number: claimData.max_claim_number,
      claim_auth_id: 0,
      claim_ms_id: memberInfo.ms_id,
      claim_user_id: 1,
      claim_provider_id: claimData.serviceProvider,
      claim_form_no: 0,
      claim_remark: claimRemark,
      claim_doc_remark: "",
      claim_prescription_date: claim_consultation_date,
      claim_auth_req_amt: 0,
      claim_discount_amt: 0,
      claim_ded_copay_amt: 0,
      claim_deduction_amt: 0,
      claim_net_amt: 0,
      claim_medical_report: null,
      claim_dd_medical_report: null,
      claim_anyreporting_doc: null,
      claim_facility_id: 0,
      claim_doctor_id: 0,
      claim_provider_type: providerType,
      claim_transaction_date: null,
      claim_provider_remark: "",
      claim_approval_status: "Approved",
      claim_approval_date: new Date(),
      claim_approval_no: "",
      claim_isDeleted: 0,
      claim_create_date: claimData.dateAdded
        ? new Date(claimData.dateAdded).toISOString().slice(0, 10)
        : new Date(),
      claim_modify_date: null,
      claim_last_modify_id: null,
      claim_lot_no: claimData.max_lot_no,
      // claim_verified_date: new Date(claimData.dateAdded).toISOString().slice(0, 10), // most of the data in the database use the same date for verified date and create date // give us a default date
      claim_verified_status: 1,
      claim_audit_status: null,
      claim_audit_date: null,
      claim_payment_status: 0,
      claim_payment_date: null,
      claim_payment_by_user: null,
      claim_type:
        claimData.typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2,
      claim_invoice_date: null,
      // claim_is_proforma_generated: 0,
      claim_medical_reviewer: "",
      claim_processed: 1,
      claim_encounter_date: claim_encounter_date,
      claim_discharge_date: claim_discharge_date,
      claim_no_stay: numberOfStays,
      claim_nn_doctor_id: 0,
      claim_no_session: 0,
      claim_external_dotor: 0,
      claim_send_supervisor: 0,
      claim_total_amount: 0,
      recovery_amt: 0,
      claim_master_type: 1,
      claim_network_provider: null,
      claim_nn_provider: null,
      claim_approve_remark: "",
      claim_invoice_number: null,
      claim_file_number: null,
      claim_gross_amt: null,
      claim_before_discount: 0,
      claim_before_copay: 0,
      claim_companion_ttl_stay: null,
      claim_companion_ttl_charge: null,
      claim_vat: 0,
      claim_payable_amt: 0,
      claim_materinity_flag: 0,
      claim_emergency: 0,
      claim_reception_date: null,
      claim_discharge_amt: null,
      claim_due_date: null,
      claim_invoice_no: null,
      claim_service_date: claim_consultation_date,
      claim_cancel_msg: null,
      // claim_external_consumables: 0,
      claim_overall_status: null,
      claim_overall_reason: null,
      claim_approved_amt: 0,
      claim_tariff_amt: 0,
      claim_exceed_amt: 0,
      claim_risk_amt: 0,
      claim_jumbo_amt: 0,
      claim_qty_claimed: quantity.length,
      claim_qty_approved: quantity.length - nonApproved,
      claim_exchange_rate: 1,
      claim_net_amt_lc: null,
      claim_exceed_amt_lc: 0,
      claim_appointment_id: null,
      claim_profession: 0,
      claim_profession_fees: 0,
      claim_reimb_provider_state_id: null,
      claim_delivery_charge: 0,
      claim_delivery_charge_fee: 0,
    });
    return result.insertId;
  } catch (error) {
    throw new Error(`Failed to insert claim: ${error.message}`);
  }
}

async function storeItemsInTables(
  connection,
  items,
  { claimId, memberInfo, provider, typeOfVisit, diagnosis, networkId },
  notFoundItemsFile
) {
  try {
    const labItems = [];
    const drugItems = [];
    const notFoundItems = [];
    const diagnosisItems = [];
    const loggedNotFoundItems = new Set();

    const ipOp = typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2;
    const claimType = typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 2 : 1;

    if (
      items.item.length !== items.quantity.length ||
      items.item.length !== items.cost.length
    ) {
      throw new Error("Mismatch in items, quantity, or cost arrays.");
    }
    const LaboratoryItems = items.mapped.services;

    // Categorize items by maintaining the original index
    for (let index = 0; index < items.item.length; index++) {
      const itemName = items.item[index];
      const itemType = items.itemType[index];

      // Check if the item is a Laboratory service
      const matchedService = LaboratoryItems.find(
        (service) => service.item === itemName
      );

      if (!matchedService) {
        continue; // Skip non-lab items
      }

      const desc = matchedService.item.replace(/^"|"$/g, "");
      const code = matchedService.code;

      const labQuery = `
        SELECT tm_id, tm_tariff_id, tm_net_amt, sl_id, sl_cover_id, tm_cpt_code AS code, tm_description AS description, tm_currency AS currency,
        (CASE 
          WHEN tm_cpt_code = '${code}' OR tm_pro_cpt_code = '${code}' THEN 'CODE'
          WHEN (tm_pro_description LIKE '%${desc}%' OR tm_description = '%${desc}%') THEN 'DESCRIPTION'
          ELSE 'UNKNOWN'
        END) AS match_type  -- Detects how the item was matched 
        FROM tariff_master 
        JOIN provider_tariff ON pt_id = tm_tariff_id AND pt_status = '1' 
        JOIN service_list ON sl_id = tm_service_id 
        WHERE tm_cpt_code = '${code}' OR tm_pro_cpt_code = '${code}' OR (tm_pro_description LIKE '%${desc}%' OR tm_description = '%${desc}%' ) 
        -- AND (pt_networkid = ? OR pt_networkid = 0) 
        -- AND (tm_claim_type = ? OR tm_claim_type = 0) 
        -- AND sl_ip_op != ? 
        LIMIT 1`;

      console.log("Checking for lab item:", matchedService);
      const [tariffData] = await connection.query(labQuery, [
        networkId,
        ipOp,
        claimType,
      ]);

      if (tariffData.length === 0) {
        console.log("No lab data found for item:", matchedService);
      } else {
        console.log("Lab data found:", tariffData);
      }
      let itemMatched = false;

      if (tariffData.length > 0) {
        const itemData = {
          code: tariffData[0].code,
          description: tariffData[0].description,
          cost: items.cost[index], // Using correct index
          matchType: tariffData[0].map_type,
          quantity: items.quantity[index], // Using correct index
          quantityApproved: items.quantityApproved[index], // Using correct index
          awarded: items.awarded[index],
          rejected: items.rejected[index],
          rejectionReason: items.rejectionReason[index],
          total: items.total[index], // Using correct index
          tm_id: tariffData[0].tm_id,
          tm_tariff_id: tariffData[0].tm_tariff_id,
          tm_net_amt: tariffData[0].tm_net_amt,
          sl_id: tariffData[0].sl_id,
          sl_cover_id: tariffData[0]?.sl_cover_id,
        };

        labItems.push(itemData);
        itemMatched = true;
      }

      if (!itemMatched && itemType !== "Drug" && itemType !== "Drugs") {
        const entry = {
          item: itemName,
          quantity: items.quantity[index],
          cost: items.cost[index],
        };
        const entryKey = JSON.stringify(entry); // Unique key to avoid duplicates
        if (!loggedNotFoundItems.has(entryKey)) {
          notFoundItems.push(entry);
          loggedNotFoundItems.add(entryKey);
        }
      }

      console.log("Items successfully pushed.");
    }

    const drugitems = items.mapped.drugs;

    // Loop over all `items.item` to maintain index integrity
    for (let index = 0; index < items.item.length; index++) {
      const itemName = items.item[index];
      const itemType = items.itemType[index];

      // Find the corresponding drug in `drugitems`
      const matchedDrug = drugitems.find((drug) => drug.item === itemName);

      if (!matchedDrug) {
        continue; // Skip non-drug items
      }

      const desc = matchedDrug.item.replace(/^"|"$/g, "");
      const code = matchedDrug.code;

      // Get tariff ID
      const tariffQuery = `
        SELECT pmt_id FROM pbm_provider_medicine_tariff 
        WHERE pmt_provider_id='${provider}' AND pmt_expiry_date IS NULL`;

      let [tariffId] = await connection.query(tariffQuery);
      tariffId = tariffId[0]?.pmt_id || null;

      console.log("Fetching Drug");

      // Main drug query
      const drugQuery = `SELECT 
          adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period,
          description, md_pro_desc, mu_id, mfo_id, form, chronic_flag, med_code,
          CONCAT_WS(' ', description, '-', trade_name, form, dosage, 
              IF(dispense != 'Unit Type', CONCAT_WS(' ', strips, dispense), '')) AS drug_details,
          med_id, unitform, dispense,
          (CASE WHEN strips IS NOT NULL OR strips = '' THEN 'Strips' ELSE '' END) AS strips,
          (CASE 
            WHEN med_code = '${code}' THEN 'CODE'
            WHEN md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%' THEN 'DESCRIPTION'
            ELSE 'UNKNOWN'
          END) AS match_type  -- Detects how the item was matched
      FROM pbm_medicine
      JOIN pbm_provider_medicine_discount ON md_med_id = med_id 
          AND (md_pro_desc != '' OR md_pro_desc = '')
      JOIN pbm_provider_medicine_tariff ON pmt_id = md_pmt_id 
          AND pmt_status = 1
      LEFT JOIN auth_drugs_prescribed ON adp_drug_code = med_code
      WHERE (med_code = '${code}' OR md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%') 
          AND pbm_medicine.status != '0' 
          AND pmt_id = '${tariffId}'
      GROUP BY med_id, md_pro_desc, pbm_provider_medicine_discount.md_id,
          adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period
      LIMIT 1;`;

      const [drugData] = await connection.query(drugQuery);

      let drug = drugData;
      console.log("🚀 ~ Drug found successfully:", drugData);

      let itemMatched = false;

      // Handle missing drug - Fallback to global search
      if (drugData.length === 0) {
        console.warn("No matching drug found for code:", matchedDrug);
        console.log("Checking for drug_items globally...");

        const globalDrugQuery = `
          SELECT 
          adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period, 
          description, md_pro_desc, mu_id, mfo_id, form, chronic_flag, med_code,
          CONCAT_WS(' ', description, '-', trade_name, form, dosage, 
              IF(dispense != 'Unit Type', CONCAT_WS(' ', strips, dispense), '')) AS drug_details,
          med_id, unitform, dispense,
          (CASE WHEN strips IS NOT NULL OR strips = '' THEN 'Strips' ELSE '' END) AS strips,
          (CASE 
            WHEN med_code = '${code}' THEN 'CODE'
            WHEN md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%' THEN 'DESCRIPTION'
            ELSE 'UNKNOWN'
          END) AS match_type  -- Detects how the item was matched
          FROM pbm_medicine
          JOIN pbm_provider_medicine_discount ON md_med_id = med_id 
              AND (md_pro_desc != '' OR md_pro_desc = '')
          JOIN pbm_provider_medicine_tariff ON pmt_id = md_pmt_id 
              AND pmt_status = 1
          LEFT JOIN auth_drugs_prescribed ON adp_drug_code = med_code
          WHERE (med_code = '${code}' OR md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%') 
              AND pbm_medicine.status != '0' 
          GROUP BY med_id, md_pro_desc, pbm_provider_medicine_discount.md_id,
              adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period
          LIMIT 1`;

        const [globalDrugData] = await connection.query(globalDrugQuery);
        drug = globalDrugData;
        console.log("🚀 ~ Global Drug Data Found:", drug);
      }

      // If a drug is found, save the details
      if (drug.length > 0) {
        const itemData = {
          code: drug[0].med_code,
          matchType: drug[0].match_type,
          description: drug[0].drug_details || drug[0].md_pro_desc,
          cost: items.cost[index], // Using correct index
          quantity: items.quantity[index], // Using correct index
          md_pro_desc: drug[0].md_pro_desc,
          quantityApproved: items.quantityApproved[index],
          total: items.total[index],
          awarded: items.awarded[index],
          rejected: items.rejected[index],
          rejectionReason: items.rejectionReason[index],
          med_code: drug[0].med_code,
          med_id: drug[0].med_id,
          unitform: drug[0].unitform,
          pud_id: drug[0].pud_id,
          adp_dosage_per_tme: drug[0].adp_dosage_per_tme,
          adp_dosage_frequency: drug[0].adp_dosage_frequency,
          adp_prescribed_period: drug[0].adp_prescribed_period,
        };

        drugItems.push(itemData);
        itemMatched = true;
      }

      // If no match was found, log missing drug details
      if (!itemMatched) {
        const entry = {
          item: itemName,
          quantity: items.quantity[index],
          cost: items.cost[index],
        };
        const entryKey = JSON.stringify(entry); // Unique key
        if (!loggedNotFoundItems.has(entryKey)) {
          notFoundItems.push(entry);
          loggedNotFoundItems.add(entryKey);
        }
      }

      console.log("Items successfully pushed.");
    }

    if (typeof diagnosis === "string") {
      diagnosis = [diagnosis];
    }
    for (let d of diagnosis) {
      d = d.replace(/^"|"$/g, ""); // Remove quotes from the string
      console.log("Checking for diagnosis:", d);
      const sql = `
      SELECT icd_id,icd_code, icd_long_description, icd_cover_id 
      FROM icd_detail_cm 
      WHERE SOUNDEX(icd_short_description) = SOUNDEX('${d}')
      OR SOUNDEX(icd_long_description) = SOUNDEX('${d}')
      LIMIT 1`;
      const [diagnosisCode] = await connection.query(sql);
      console.log("🚀 ~ diagnosisCode:", diagnosisCode);

      if (diagnosisCode.length > 0) {
        diagnosisItems.push({
          code: diagnosisCode[0].icd_code,
          description: diagnosisCode[0].icd_long_description,
        });
      } else {
        console.warn("No matching diagnosis code found for:", d);
        const entry = { diagnosis: d };
        const entryKey = JSON.stringify(entry); // unique key
        if (!loggedNotFoundItems.has(entryKey)) {
          console.log("Doesn't have entry");
          notFoundItems.push(entry);
          console.log("push entry");
          loggedNotFoundItems.add(entryKey); // Avoid duplicate entries
          console.log("log entry");
          continue;
        }
      }
    }
    console.log("🚀 ~ diagnosisItems:", diagnosisItems);

    await insertLabItems(connection, labItems, claimId);
    await insertDrugItems(connection, drugItems, claimId, memberInfo);
    await insertDiagnosis(connection, diagnosisItems, claimId);
    await updateMissingDiagnosis(
      connection,
      diagnosisItems,
      claimId,
      notFoundItemsFile
    );
    console.log("Checking for duplicate dignosis");
    await removeDuplicateDiagnosis(connection, claimId);

    if (notFoundItems.length > 0) {
      console.log("Logging not found items...");
      await logNotFoundItems(notFoundItemsFile, notFoundItems);
    }

    // try {
    //   console.log("Updating final claim result.....");
    //   await updateClaim(connection, labItems, drugItems, claimId, items);
    // } catch (e) {
    //   throw new Error(`Failed to update claim: ${e}`);
    // }
  } catch (error) {
    throw new Error(`Failed to store items: ${error}`);
  }
}

async function removeDuplicateDiagnosis(connection, claimId) {
  try {
    const deleteQuery = `
      UPDATE claim_codes c1
        INNER JOIN claim_codes c2 
        ON c1.cc_claim_id = c2.cc_claim_id 
        AND c1.cc_code = c2.cc_code
        AND c1.cc_id > c2.cc_id
        SET c1.cc_claim_id = 0
        WHERE c1.cc_claim_id = ?
    `;
    const [result] = await connection.query(deleteQuery, [claimId]);
    console.log(`Ra2ma4va2da ${result.affectedRows}: claim ${claimId}`);
  } catch (error) {
    console.error("Error removing duplicate diagnosis records:", error);
    throw error;
  }
}

async function updateMissingDiagnosis(
  connection,
  diagnosisItems,
  claimId,
  notFoundItemsFile
) {
  console.log("🚀 ~ updateMissingDiagnosis ~ claimId:", claimId);
  try {
    const diagnosisQuery = `SELECT cc_code_des FROM claim_codes WHERE cc_claim_id = ${claimId}`;
    const dbDiagnosis = await connection.query(diagnosisQuery);
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ diagnosisQuery:",
      diagnosisQuery
    );
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ dbDiagnosis[0]:",
      dbDiagnosis[0]
    );
    const dbDiagnosisDescriptions = new Set(
      dbDiagnosis[0]?.map((d) => d.cc_code_des.toLowerCase()) || []
    );
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ dbDiagnosisDescriptions:",
      dbDiagnosisDescriptions
    );
    diagnosisItems =
      diagnosisItems.map((d) => d.description.toLowerCase()) || [];
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ diagnosisItems:",
      diagnosisItems
    );

    const missingDiagnosis = diagnosisItems.filter(
      (d) => !dbDiagnosisDescriptions.has(d.toLowerCase())
    );
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ missingDiagnosis:",
      missingDiagnosis
    );
    for (const diag of missingDiagnosis) {
      let diagRecord;
      console.log("🚀 ~ updateMissingDiagnosis ~ diag:", diag);
      const diagQuery = `SELECT icd_id,icd_code as code, icd_long_description as description, icd_cover_id 
        FROM icd_detail_cm 
        WHERE icd_short_description = '${diag}'
        OR icd_long_description = '${diag}'
        LIMIT 1`;
      [diagRecord] = await connection.query(diagQuery);
      console.log("🚀 ~ updateMissingDiagnosis ~ diagRecord:", diagRecord);

      if (diagRecord.length === 0) {
        console.log("Fetching diagnosis globally...");
        const diagQuery = `
          SELECT * 
          FROM icd_detail_cm
          WHERE (SOUNDEX(icd_short_description) = SOUNDEX('${diag}') 
          OR SOUNDEX(icd_long_description) = SOUNDEX('${diag}'))
          AND (icd_short_description LIKE '%${diag}%' 
          OR icd_long_description LIKE '%${diag}%')`;
        [diagRecord] = await connection.query(diagQuery);
        console.log("🚀 ~ updateMissingDiagnosis ~ diagRecord:", diagRecord);
      }

      if (diagRecord.length) {
        console.log("Diagnosis found, creating new record");
        await connection.query("INSERT INTO claim_codes SET ?", {
          cc_claim_id: claimData.claim_id,
          cc_auth_id: 0,
          cc_code: diagRecord[0].code,
          cc_code_des: diagRecord[0].description,
          cc_code_etc: null,
          cc_code_type: "ICD",
          cc_status: "Approved",
          cc_remark: "",
          cc_is_skip: 0,
          cc_den_code: null,
          cc_den_code_des: null,
          cc_add_den_code_desc: null,
        });
      } else {
        await logNotFoundItems(notFoundItemsFile, {
          type: "diagnosis",
          item: diag,
        });
      }
    }
  } catch (e) {
    console.error("Error updating diagnosis", e);
    throw e;
  }
}

// async function updateClaim(connection, labItems, drugItems, claimId) {
//   console.log("🚀 ~ updateClaim ~ labItems:", labItems);
//   console.log("🚀 ~ updateClaim ~ drugItems:", drugItems);

//   const combinedItems = [...labItems, ...drugItems];

//   const totalTariffAmt = combinedItems.reduce((total, item) => {
//     return total + Number(item.cost) * Number(item.quantity);
//   }, 0);
//   console.log("🚀 ~ totalTariffAmt:", totalTariffAmt);

//   const totalApprovedAmt = combinedItems.reduce((total, item) => {
//     return total + Number(item.awarded);
//   }, 0);
//   console.log("🚀 ~ totalApprovedAmt:", totalApprovedAmt);

//   const totalClaimedAmt = combinedItems.reduce((total, item) => {
//     return total + Number(item.total);
//   }, 0);
//   console.log("🚀 ~ totalClaimedAmt:", totalClaimedAmt);

//   const totalDeductionAmt = totalClaimedAmt - totalApprovedAmt;
//   console.log("🚀 ~ totalDeductionAmt:", totalDeductionAmt);

//   // Uncomment to update database:
//   const [result] = await connection.query(
//     `UPDATE claim SET ? WHERE claim_id = ?`,
//     [
//       {
//         claim_tariff_amt: totalTariffAmt,
//         claim_net_amt: totalApprovedAmt,
//         claim_approved_amt: totalApprovedAmt,
//         claim_risk_amt: totalApprovedAmt,
//         claim_auth_req_amt: totalClaimedAmt,
//         claim_no_session: totalClaimedAmt,
//         claim_total_amount: totalClaimedAmt,
//         claim_payable_amt: totalClaimedAmt,
//         claim_deduction_amt: totalDeductionAmt,
//       },
//       claimId,
//     ]
//   );
// }

async function insertLabItems(connection, labItems, claimId) {
  try {
    for (const labItem of labItems) {
      await connection.query("INSERT INTO claim_details SET ?", {
        cd_claim_id: claimId,
        cd_auth_id: 0,
        cd_cover_id: labItem.sl_cover_id,
        cd_plan_id: 0,
        cd_activity_type: labItem.sl_id,
        cd_tooth: "",
        cd_activity_code: labItem.code,
        cd_activity_des: labItem.description,
        cd_activity_req_amt: labItem.total,
        cd_discount_amt: 0,
        cd_ded_copay_amt: 0,
        cd_deduction_amt: labItem.total - labItem.awarded,
        cd_qty_claimed: labItem.quantity,
        cd_qty_approved: labItem.quantityApproved,
        cd_claimed_amt: labItem.cost,
        cd_approved_amt: labItem.awarded,
        cd_recovery: 0,
        cd_activity_net_amt: labItem.awarded,
        cd_activity_remark: "",
        cd_eye: null,
        cd_cylinder: null,
        cd_spine: null,
        cd_access: null,
        cd_pack_price: null,
        cd_no_session: null,
        // cd_auth_approval: labItem.cost - labItem.cost === 0 ? 1 : 0,
        cd_auth_approval: labItem.rejectionReason !== "Approved" ? 0 : 1,
        cd_stay: null,
        cd_risk_net_amt: labItem.awarded,
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
        cd_add_den_code_desc:
          labItem.rejectionReason === "Approved" ? "" : labItem.rejectionReason,
        cd_service_date: null,
        cd_activity_net_qty: null,
        cd_net_amt_paid: null,
        cd_extra_flag: null,
        cd_tariff_amt: labItem.tm_net_amt,
        cd_currency: labItem.currency,
        cd_exchange_rate: 1,
      });

      console.log(
        `Lab item successfully inserted - code: ${labItem.code} desc: ${labItem.description}, claimId: ${claimId}`
      );
    }
  } catch (error) {
    console.error("Error inserting lab items:", error.message);
  }
}

async function insertDrugItems(connection, drugItems, claimId, memberInfo) {
  const unitDrops = await getUnitDrops(connection);
  const pud_id = unitDrops[drugItems[0].unitform][0].pud_id;
  console.log("🚀 ~ updateMissingDrugs ~ pud_id:", pud_id);
  try {
    for (const drugItem of drugItems) {
      const {
        md_pro_desc,
        med_code,
        med_id,
        unitform,
        adp_dosage_per_tme,
        adp_prescribed_period,
        adp_dosage_frequency,
      } = drugItem;

      await connection.query("INSERT INTO claim_drugs_prescribed SET ?", {
        cdp_claim_id: claimId,
        cdp_auth_id: 0,
        cdp_cover_id: "112",
        cdp_ms_id: memberInfo.ms_id,
        cdp_pro_desc: md_pro_desc === "" ? drug_details : md_pro_desc,
        cdp_drug_code: med_code,
        cdp_dosage_per_tme: adp_dosage_per_tme || 1,
        cdp_dosage_unit: pud_id,
        cdp_dosage_frequency: adp_dosage_frequency || 1,
        cdp_prescribed_by: "Day",
        cdp_prescribed_period: adp_prescribed_period || 1,
        cdp_tot_dosage_prescribed: drugItem.quantity,
        cdp_tot_dosage_prescribed_unit: unitform,
        cdp_dispense_unit: drugItem.quantity,
        cdp_dispense_type: unitform,
        cdp_dispense_unit1: 0,
        cdp_dispense_type1: null,
        cdp_gross_amount: drugItem.total,
        cdp_discount: 0,
        cdp_copay: 0,
        cdp_net_price: drugItem.awarded,
        cdp_net_price_bill: drugItem.awarded,
        cdp_remark: "",
        // cdp_status: drugItem.rejectionReason !== 'Approved' ? 0 : 1,
        // cdp_status: '1',
        cdp_created_date: new Date().toISOString().slice(0, 10),
        cdp_modify_date: null,
        cdp_extra_dispense: `0 ${unitform}`,
        cdp_isdispense: 0,
        cdp_drug_dispenseby: 0,
        cdp_dispense_date: null,
        // cdp_drug_status: 'Approved',
        cdp_drug_status:
          drugItem.rejectionReason !== "Approved" ? "Denied" : "Approved",
        cdp_is_skip: 0,
        cdp_den_code: null,
        cdp_den_code_des: null,
        cdp_add_den_code_desc:
          drugItem.rejectionReason === "Approved"
            ? ""
            : drugItem.rejectionReason,
        cdp_approve_amt: drugItem.awarded,
        // cdp_approve_amt: drugItem.cost,
        cdp_changed_price: 0,
        cdp_qty_claimed_val: drugItem.quantity,
        cdp_qty_claimed_unit: unitform,
        cdp_qty_approved_val: drugItem.quantityApproved,
        cdp_qty_approved_unit: null,
        // cdp_qty_approved_val: 1,
        // cdp_qty_approved_unit: unitform,
        cdp_final_price: drugItem.total,
        cdp_drug_price: drugItem.cost,
        cdp_deduction_amt: drugItem.total - drugItem.awarded,
        cdp_exceeding_limitation: 0,
        cdp_debited_amt: 0,
        cdp_risk_recovery_amt: 0,
        cdp_risk_carrier_net_amt: drugItem.awarded,
        cdp_exgratia: 0,
      });

      console.log(
        `Drug item successfully inserted - code: ${med_code} desc: ${drugItem.description}, claimId: ${claimId}`
      );
    }
  } catch (error) {
    console.error("Error inserting drug items:", error.message);
  }
}

async function insertDiagnosis(connection, diagnosisItems, claimId) {
  try {
    for (let diagnosis of diagnosisItems) {
      await connection.query("INSERT INTO claim_codes SET ?", {
        cc_claim_id: claimId,
        cc_auth_id: 0,
        cc_code: diagnosis.code,
        cc_code_des: diagnosis.description,
        cc_code_etc: null,
        cc_code_type: "ICD",
        cc_status: "Approved",
        cc_remark: "",
        cc_is_skip: 0,
        cc_den_code: null,
        cc_den_code_des: null,
        cc_add_den_code_desc: null,
      });

      console.log(
        `Diagnosis successfully inserted - code: ${diagnosis.code} desc: ${diagnosis.description}, claimId: ${claimId}`
      );
    }
  } catch (error) {
    console.error("Error inserting diagnosis:", error.message);
  }
}

async function logNotFoundItems(filePath, data) {
  try {
    const existingData = await fs.readFile(filePath, "utf-8");
    const records = existingData ? JSON.parse(existingData) : [];
    if (!Array.isArray(records)) {
      throw new Error("Invalid data format: expected an array");
    }
    records.push(...data);
    await fs.writeFile(filePath, JSON.stringify(records, null, 2), "utf-8");
  } catch (error) {
    console.error("Error logging failed record:", error);
  }
}

async function getProviderNetworkId(connection, providerId, ms_plan_network) {
  console.log("🚀 ~ getProviderNetworkId ~ ms_plan_network:", ms_plan_network);
  const providerNetworkQuery = `
      SELECT pt_networkid, pt_id 
      FROM provider_tariff 
      WHERE pt_status = '1' AND pt_provider_id = ?`;
  const [network] = await connection.query(providerNetworkQuery, [providerId]);
  console.log("🚀 ~ getProviderNetworkId ~ network:", network);

  console.log(
    "🚀 ~ getProviderNetworkId ~ network[0]?.pt_networkid:",
    network[0]?.pt_networkid
  );
  const networkId =
    network[0]?.pt_networkid != (0 || undefined)
      ? network[0]?.pt_networkid
      : ms_plan_network != 0
        ? ms_plan_network
        : 0;
  console.log("🚀 ~ getProviderNetworkId ~ networkId:", networkId);

  return { networkId };
}

function formatDuration(duration) {
  const milliseconds = parseInt((duration % 1000) / 100);
  const seconds = Math.floor((duration / 1000) % 60);
  const minutes = Math.floor((duration / (1000 * 60)) % 60);
  const hours = Math.floor((duration / (1000 * 60 * 60)) % 24);
  return `${hours}h ${minutes}m ${seconds}s ${milliseconds}ms`;
}

function calculateHospitalStay(dateOfAdmission, dateOfDischarge) {
  if (!dateOfAdmission || !dateOfDischarge) return 0;
  const admissionDate = new Date(dateOfAdmission);
  const dischargeDate = new Date(dateOfDischarge);
  const diffTime = dischargeDate.getTime() - admissionDate.getTime();
  return Math.max(0, Math.ceil(diffTime / (1000 * 60 * 60 * 24))); // Convert milliseconds to days
}

async function findMemberInfo(connection, memberNumber, memberNumber1) {
  try {
    memberNumber = String(memberNumber);
    memberNumber1 = String(memberNumber1);
    console.log("🚀 ~ findMemberInfo ~ uniqueId's:", {
      memberNumber,
      memberNumber1,
    });
    const [member] = await connection.query(
      `
      SELECT ms_id, ms_tob_plan_id, ms_plan_network, mm_member_id, mm_member_id, ms_policy_id
      FROM members
      INNER JOIN members_schemes ms ON ms.ms_member_id = members.mm_id
      WHERE (mm_nin_number = ? OR mm_national_id = ? OR mm_member_id = ?) 
      ORDER BY ms_id DESC LIMIT 1`,
      [memberNumber, memberNumber, memberNumber1]
    );
    console.log("🚀 ~ findMemberInfo ~ member:", member);
    if (!member || !member.length) {
      throw new Error(`No member found for memberNumber: ${memberNumber}`);
    }
    return member[0];
  } catch (error) {
    console.log(`Failed to fetch member info: ${error.message}`);
  }
}

async function findLastClaimAndLotNumber(connection) {
  try {
    const [claim] = await connection.query(
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

async function getProviderInfo(connection, providerId) {
  const providerQuery = `
  SELECT provider_id, provider_name, provider_type 
  FROM providers 
  WHERE provider_name LIKE '%${providerId}%' OR provider_id = '${providerId}'
  LIMIT 1;
  `;

  // const likeValue = `%${providerId}%`; // Add wildcards around the providerId
  const [provider] = await connection.query(providerQuery);

  console.log("🚀 ~ getProviderInfo ~ provider:", provider);
  return provider;
}

async function getUnitDrops(connection) {
  try {
    const unitDrops = await connection.query(
      "SELECT pud_id, pud_unit, pud_value FROM pbm_unit_details"
    );
    console.log("🚀 ~ getUnitDrops ~ unitDrops:", unitDrops);
    let unitWiseDrops = {};

    if (Array.isArray(unitDrops[0]) && unitDrops[0].length > 0) {
      unitDrops[0].forEach((drop) => {
        console.log("🚀 ~ getUnitDrops ~ drop:", drop);
        if (!unitWiseDrops[drop.pud_unit]) {
          unitWiseDrops[drop.pud_unit] = [];
        }
        unitWiseDrops[drop.pud_unit].push(drop);
        console.log(
          "🚀 ~ getUnitDrops ~ unitWiseDrops[drop.pud_unit]:",
          unitWiseDrops[drop.pud_unit]
        );
      });
      console.log("🚀 ~ getUnitDrops ~ unitWiseDrops:", unitWiseDrops);
    }

    return unitWiseDrops;
  } catch (error) {
    console.error("Error fetching unit drops:", error);
    return {};
  }
}

async function maxLotNumber(connection) {
  try {
    const [lot] = await connection.query(
      `
      SELECT MAX(lot_no) AS max_lot_no
      FROM lots
      WHERE lot_master_type = 1`
    );
    return {
      max_lot_no: lot?.[0]?.max_lot_no + 1 || 100000000,
    };
  } catch (error) {}
}

async function checkAndSegregateFailedRecords(failedRecordsFile) {
  try {
    const data = await fs.readFile(failedRecordsFile, "utf-8");
    const records = JSON.parse(data);
    if (records.length > 0) {
      console.log(`Found ${records.length} failed records to segregate.`);
      await segregateFailedRecords(failedRecordsFile);
    } else {
      console.log("No failed records to segregate.");
    }
  } catch (error) {
    console.error("Error checking failed records:", error);
  }
}

async function logSuccessfulRecord(filePath, record, claim_number) {
  try {
    console.log("Attempting to log successful record:", record);
    const existingData = await fs.readFile(filePath, "utf-8");
    const records = existingData ? JSON.parse(existingData) : [];
    if (!Array.isArray(records)) {
      throw new Error("Invalid data format: expected an array");
    }
    const recordWithClaimId = { ...record, claim_number };
    records.push(recordWithClaimId);
    await fs.writeFile(filePath, JSON.stringify(records, null, 2), "utf-8");
    console.log("Successfully logged record to:", filePath);
  } catch (error) {
    console.error("Error logging successful record:", error);
  }
}

// --- date helper -----------------------------------------------------------
function addOneDay(dateStr) {
  if (!dateStr) return dateStr; // keep null / '' untouched

  const d = new Date(dateStr); // parse
  if (Number.isNaN(d.getTime())) return dateStr; // invalid date → leave as‑is

  d.setDate(d.getDate() + 1); // +1 day
  return d.toISOString().slice(0, 10); // YYYY‑MM‑DD
}
// ---------------------------------------------------------------------------

module.exports = { migrateClaimDec };
