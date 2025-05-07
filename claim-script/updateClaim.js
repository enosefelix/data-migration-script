const db = require("../db");
const fs = require("fs").promises;
const path = require("path");
const { segregateFailedRecords } = require("./segregateFailedRecords");
const { logFailedRecord } = require("./failed-record.logger");

async function updateClaims(
  file,
  failedRecordsFile,
  successfulRecordsFile,
  notFoundItemsFile
) {
  console.log("🚀 ~ updateClaims ~ notFoundItemsFile:", notFoundItemsFile);
  const batchResults = {
    processed: 0,
    failed: 0,
    skipped: 0,
  };
  try {
    const startTime = new Date();
    const jsonData = await readJsonFile(file);
    console.log(jsonData.length);

    // console.log("Updating deleted status of previously migrated lot statements...")
    // await db.query(
    //     "UPDATE lots SET lot_deleted ='1' WHERE (lot_remark = 'Migrated' OR lot_remark = 'Migrated2' OR lot_remark = 'Migrated3' OR lot_remark = 'Migrated4')"
    // );
    for (const record of jsonData) {
      try {
        const processed = await processRecord(record, notFoundItemsFile);
        batchResults.processed++;
        await logSuccessfulRecord(successfulRecordsFile, record);
      } catch (e) {
        console.error(`Error processing record: ${e.message}`);
        await logFailedRecord(failedRecordsFile, {
          record,
          error: e.message,
          stackTrace: e.stack,
        });
        batchResults.failed++;
      }
    }
    console.log(
      JSON.stringify(
        {
          startTime,
          endTime: new Date(),
          duration: formatDuration(new Date() - startTime),
          batchResults,
        },
        null,
        2
      )
    );
    await checkAndSegregateFailedRecords(failedRecordsFile);
  } catch (error) {
    console.error("Error updating claims", error);
    throw error;
  } finally {
    process.exit();
  }
}

async function processRecord(record, notFoundItemsFile) {
  // console.log("🚀 ~ processRecord ~ record:", record)
  let connection;
  const claimRemark = "Migrated";
  try {
    connection = await db.getConnection();
    await connection.beginTransaction();
    console.log("🚀 ~ updateClaims ~ record:", record.typeOfVisit);
    record.typeOfVisit = normalizeTypeOfVisit(record.typeOfVisit);

    const member = await findMemberInfo(connection, record.memberNumber);

    // Fetch claim details from DB
    const claimData = await fetchClaimFromDB(connection, record.claim_number);
    if (!claimData) {
      console.log("Claim not found");
      throw new Error("Claim not found");
    }

    // Validate and update missing diagnoses, drugs, and services
    await updateMissingEntries(
      connection,
      record,
      claimData,
      notFoundItemsFile
    );

    const lot = await getOrCreateLotStatement(
      connection,
      member,
      record,
      claimRemark
    );
    // console.log("🚀 ~ processRecord ~ lot:", lot);

    // Determine the record's type (1 for IN-PATIENT, 2 for OUT-PATIENT)
    const recordType =
      record.typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2;

    // Update the lot type if necessary:
    // This query updates the lot to 0 (mixed) if the current lot_type does not match the record's type
    // and is not already 0.
    await connection.query(
      "UPDATE lots SET lot_type = 0 WHERE lot_no = ? AND lot_type <> ? AND lot_type <> 0",
      [lot, recordType]
    );

    await insertAuditAndDataEntry(connection, lot);

    await connection.query(
      "UPDATE lots SET lot_total_claim = IFNULL(lot_total_claim, 0) + 1 WHERE lot_no = ?",
      [lot]
    );

    // Recalculate and update claim totals
    await updateClaimTotals(connection, claimData.claim_id, record, lot);

    await connection.commit();
    console.log("Transaction committed successfully");
  } catch (error) {
    console.error("Error in processRecord:", {
      message: error.message,
      stack: error.stack,
      record: record,
    });
    // Rollback the transaction if an error occurs.
    if (connection) {
      try {
        await connection.rollback();
        console.log("Transaction rolled back successfully");
      } catch (rollbackError) {
        console.error("Error during rollback:", rollbackError);
        throw error;
      }
    }
    throw error;
  } finally {
    console.log("Update complete.");
    // Always release the connection.
    if (connection) connection.release();
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

async function fetchClaimFromDB(connection, claimNumber) {
  console.log("🚀 ~ fetchClaimFromDB ~ claimNumber:", claimNumber);
  const [result] = await connection.query(
    `SELECT claim_id, claim_provider_id as provider_id, claim_ms_id, claim_service_date FROM claim WHERE claim_number = ?`,
    [claimNumber]
  );
  return result.length ? result[0] : null;
}

async function updateMissingEntries(
  connection,
  record,
  claimData,
  notFoundItemsFile
) {
  try {
    await updateMissingDiagnosis(
      connection,
      record,
      claimData,
      notFoundItemsFile
    );
    // await updateMissingDrugs(connection, record, claimData, notFoundItemsFile);
    // await updateMissingServices(connection, record, claimData, notFoundItemsFile);
    console.log("Checking for duplicate dignosis");
    await removeDuplicateDiagnosis(connection, claimData.claim_id);
  } catch (e) {
    console.error("Error updating missing entries", e);
    throw e;
  }
}

async function updateMissingDiagnosis(
  connection,
  record,
  claimData,
  notFoundItemsFile
) {
  console.log("🚀 ~ updateMissingDiagnosis ~ claimData:", claimData);
  try {
    const diagnosisQuery = `SELECT cc_code_des FROM claim_codes WHERE cc_claim_id = ${claimData.claim_id}`;
    const dbDiagnosis = await connection.query(diagnosisQuery);
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ diagnosisQuery:",
      diagnosisQuery
    );
    console.log("🚀 ~ updateMissingDiagnosis ~ dbDiagnosis:", dbDiagnosis[0]);
    const dbDiagnosisDescriptions = new Set(
      dbDiagnosis[0]?.map((d) => d.cc_code_des.toLowerCase()) || []
    );
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ dbDiagnosisCodes:",
      dbDiagnosisDescriptions
    );
    console.log(
      "🚀 ~ updateMissingDiagnosis ~ record.diagnosis:",
      record.diagnosis
    );

    const missingDiagnosis = record.diagnosis.filter(
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

// async function updateMissingDrugs(connection, record, claimData, notFoundItemsFile) {
//   try {
//     // const ipOp = record.typeOfVisit === ("OUT-PATIENT" || "OutPatient") ? 1 : 2;
//     // const providerInfo = await getProviderInfo(connection, claimData.provider_id);
//     // console.log("🚀 ~ updateMissingDrugs ~ providerInfo:", providerInfo);

//     // const icd_codesDrugs = record.mapped.drugs.map(drug => {
//     //     if(!drug.code.startsWith("Entry: ")) {
//     //         return drug.code
//     //     } else {
//     //         return ''
//     //     }
//     // });
//     // console.log("🚀 ~ updateMissingDrugs ~ icd_codesDrugs:", icd_codesDrugs);

//     // const icd_codesServices = record.mapped.services.map(service => {
//     //     if(!service.code.startsWith("Entry: ")) {
//     //         return service.code
//     //     } else {
//     //         return ''
//     //     }
//     // });
//     // console.log("🚀 ~ updateMissingDrugs ~ icd_codesServices:", icd_codesServices);

//     // const coverId = await getPharmacyCoverId(connection, providerInfo[0].provider_type, ipOp, 0, [...icd_codesDrugs, ...icd_codesServices]);
//     // console.log("🚀 ~ updateMissingDrugs ~ coverId:", coverId);

//     const unitDrops = await getUnitDrops(connection)

//     const dbDrugs = await connection.query(
//     `SELECT cdp_drug_code FROM claim_drugs_prescribed WHERE cdp_claim_id = ?`,
//     [claimData.claim_id]
//   );
//     console.log("🚀 ~ updateMissingDrugs ~ dbDrugs:", dbDrugs)
//   const dbDrugCodes = new Set(dbDrugs[0].map(d => d.cdp_drug_code));
//   console.log("🚀 ~ updateMissingDrugs ~ dbDrugCodes:", dbDrugCodes)

//   const missingDrugs = record.mapped.drugs.filter(d => !dbDrugCodes.has(d.code));
//   console.log("🚀 ~ updateMissingDrugs ~ missingDrugs:", missingDrugs)
//   for (const drug of missingDrugs) {
//     console.log("🚀 ~ updateMissingDrugs ~ drug:", drug);
//     const code = drug.code;
//     console.log("🚀 ~ updateMissingDrugs ~ code:", code)
//     const desc = drug.item;
//     console.log("🚀 ~ updateMissingDrugs ~ desc:", desc);

//     const index = record.item.findIndex(item => item === drug.item);
//     console.log("🚀 ~ updateMissingDrugs ~ record.item:", record.item)

//     const quantity = record.quantity[index];
//     console.log("🚀 ~ updateMissingDrugs ~ quantity:", quantity)
//     const cost = record.cost[index];
//     console.log("🚀 ~ updateMissingDrugs ~ cost:", cost)
//     const total = record.total[index];
//     console.log("🚀 ~ updateMissingDrugs ~ total:", total)
//     const awarded = record.awarded[index];
//     console.log("🚀 ~ updateMissingDrugs ~ awarded:", awarded)
//     const rejected = record.rejected[index];
//     console.log("🚀 ~ updateMissingDrugs ~ rejected:", rejected)
//     const rejectionReason = record.rejectionReasons[index];
//     console.log("🚀 ~ updateMissingDrugs ~ rejectionReason:", rejectionReason)
//     const quantityApproved = record.quantityApproved[index];
//     console.log("🚀 ~ updateMissingDrugs ~ quantityApproved:", quantityApproved)

//     const tariffQuery = `
//         SELECT pmt_id FROM pbm_provider_medicine_tariff
//         WHERE pmt_provider_id='${claimData.provider_id}' AND pmt_expiry_date IS NULL`;

//     let [tariffId] = await connection.query(tariffQuery);
//     tariffId = tariffId[0]?.pmt_id || null;
//     console.log("🚀 ~ updateMissingDrugs ~ tariffId:", tariffId)

//     const drugQuery = `SELECT
//           adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period,
//           description, md_pro_desc, mu_id, mfo_id, form, chronic_flag, med_code,
//           CONCAT_WS(' ', description, '-', trade_name, form, dosage,
//               IF(dispense != 'Unit Type', CONCAT_WS(' ', strips, dispense), '')) AS drug_details,
//           med_id, unitform, dispense,
//           (CASE WHEN strips IS NOT NULL OR strips = '' THEN 'Strips' ELSE '' END) AS strips,
//           (CASE
//             WHEN med_code = '${code}' THEN 'CODE'
//             WHEN md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%' THEN 'DESCRIPTION'
//             ELSE 'UNKNOWN'
//           END) AS match_type  -- Detects how the item was matched
//       FROM pbm_medicine
//       JOIN pbm_provider_medicine_discount ON md_med_id = med_id
//           AND (md_pro_desc != '' OR md_pro_desc = '')
//       JOIN pbm_provider_medicine_tariff ON pmt_id = md_pmt_id
//           AND pmt_status = 1
//       LEFT JOIN auth_drugs_prescribed ON adp_drug_code = med_code
//       WHERE (med_code = '${code}' OR md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%')
//           AND pbm_medicine.status != '0'
//           AND pmt_id = '${tariffId}'
//       GROUP BY med_id, md_pro_desc, pbm_provider_medicine_discount.md_id,
//           adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period
//       LIMIT 1`;

//     const [drugRecord] = await connection.query(drugQuery);
//     let drugI = drugRecord;
//     console.log("🚀 ~ updateMissingDrugs ~ drugI:", drugI)
//     console.log("🚀 ~ updateMissingDrugs ~ drugI[0].unitform:", drugI[0]?.unitform);
//     console.log("🚀 ~ updateMissingDrugs ~ drugI[0].unitform:", unitDrops[drugI[0]?.unitform]);

//     if (drugRecord.length === 0) {
//         console.warn("No matching drug found for:", {code: code, name: desc});
//         console.log("Checking for drug_items globally...");

//         const globalDrugQuery = `
//         SELECT
//         adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period,
//         description, md_pro_desc, mu_id, mfo_id, form, chronic_flag, med_code,
//         CONCAT_WS(' ', description, '-', trade_name, form, dosage,
//             IF(dispense != 'Unit Type', CONCAT_WS(' ', strips, dispense), '')) AS drug_details,
//         med_id, unitform, dispense,
//         (CASE WHEN strips IS NOT NULL OR strips = '' THEN 'Strips' ELSE '' END) AS strips,
//         (CASE
//             WHEN med_code = '${code}' THEN 'CODE'
//             WHEN md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%' THEN 'DESCRIPTION'
//             ELSE 'UNKNOWN'
//         END) AS match_type  -- Detects how the item was matched
//         FROM pbm_medicine
//         JOIN pbm_provider_medicine_discount ON md_med_id = med_id
//             AND (md_pro_desc != '' OR md_pro_desc = '')
//         JOIN pbm_provider_medicine_tariff ON pmt_id = md_pmt_id
//             AND pmt_status = 1
//         LEFT JOIN auth_drugs_prescribed ON adp_drug_code = med_code
//         WHERE (med_code = '${code}' OR md_pro_desc LIKE '%${desc}%' OR description LIKE '%${desc}%')
//             AND pbm_medicine.status != '0'
//         GROUP BY med_id, md_pro_desc, pbm_provider_medicine_discount.md_id,
//             adp_dosage_per_tme, adp_dosage_frequency, adp_prescribed_period
//         LIMIT 1`;

//         const [globalDrugData] = await connection.query(globalDrugQuery);
//         drugI = globalDrugData;
//         console.log("🚀 ~ Global Drug Data Found:", drugI);
//         console.log("🚀 ~ Global Drug Data Found:", drugI[0]?.unitform);
//     }

//     if (drugI.length) {
//         const pud_id = unitDrops[drugI[0].unitform][0].pud_id;
//         console.log("🚀 ~ updateMissingDrugs ~ pud_id:", pud_id)
//         await connection.query("INSERT INTO claim_drugs_prescribed SET ?", {
//             cdp_claim_id: claimData.claim_id,
//             cdp_auth_id: 0,
//             cdp_cover_id: '112',
//             cdp_ms_id: claimData.claim_ms_id,
//             cdp_pro_desc: drugI[0].md_pro_desc,
//             cdp_drug_code: drugI[0].med_code,
//             cdp_dosage_per_tme: drugI[0].adp_dosage_per_tme || 1,
//             cdp_dosage_unit: pud_id,
//             cdp_dosage_frequency: drugI[0].adp_dosage_frequency || 1,
//             cdp_prescribed_by: 'Day',
//             cdp_prescribed_period: drugI[0].adp_prescribed_period || 1,
//             cdp_tot_dosage_prescribed: quantity,
//             cdp_tot_dosage_prescribed_unit: drugI[0].unitform,
//             cdp_dispense_unit: quantity,
//             cdp_dispense_type: drugI[0].unitform,
//             cdp_dispense_unit1: 0,
//             cdp_dispense_type1: null,
//             cdp_gross_amount: total,
//             cdp_discount: 0,
//             cdp_copay: 0,
//             cdp_net_price: awarded,
//             cdp_net_price_bill: awarded,
//             cdp_remark: "",
//             // cdp_status: rejectionReason !== 'Approved' ? 0 : 1,
//             // cdp_status: '1',
//             cdp_created_date: new Date().toISOString().slice(0, 10),
//             cdp_modify_date: null,
//             cdp_extra_dispense: `0 ${drugI[0].unitform}`,
//             cdp_isdispense: 0,
//             cdp_drug_dispenseby: 0,
//             cdp_dispense_date: null,
//             // cdp_drug_status: 'Approved',
//             cdp_drug_status: rejectionReason !== 'Approved' ? "Denied" : "Approved",
//             cdp_is_skip: 0,
//             cdp_den_code: null,
//             cdp_den_code_des: null,
//             cdp_add_den_code_desc: rejectionReason === 'Approved' ? '' : rejectionReason,
//             cdp_approve_amt: awarded,
//             // cdp_approve_amt: cost,
//             cdp_changed_price: 0,
//             cdp_qty_claimed_val: quantity,
//             cdp_qty_claimed_unit: drugI[0].unitform,
//             cdp_qty_approved_val: quantityApproved,
//             cdp_qty_approved_unit: null,
//             // cdp_qty_approved_val: 1,
//             // cdp_qty_approved_unit: unitform,
//             cdp_final_price: total,
//             cdp_drug_price: cost,
//             cdp_deduction_amt: total - awarded,
//             cdp_exceeding_limitation: 0,
//             cdp_debited_amt: 0,
//             cdp_risk_recovery_amt: 0,
//             cdp_risk_carrier_net_amt: awarded,
//             cdp_exgratia: 0,
//         });
//     } else {
//       await logNotFoundItems(notFoundItemsFile, { type: "drug", item: drug.item });
//     }
//   }
//   } catch (e) {
//     console.error("Error updating missing drugs", e);
//     throw e;
//   }
// }

// async function updateMissingServices(connection, record, claimData, notFoundItemsFile) {
// //   console.log("🚀 ~ updateMissingServices ~ record:", record)
//   try {
//     const dbServices = await connection.query(
//     `SELECT cd_activity_code FROM claim_details WHERE cd_claim_id = ?`,
//     [claimData.claim_id]
//   );
//     console.log("🚀 ~ updateMissingServices ~ dbServices:", dbServices)
//   const dbServiceCodes = new Set(dbServices[0].map(s => s.cd_activity_code));
//   console.log("🚀 ~ updateMissingServices ~ dbServiceCodes:", dbServiceCodes)

//   const missingServices = record.mapped.services.filter(s => !dbServiceCodes.has(s.code));
//   console.log("🚀 ~ updateMissingServices ~ missingServices:", missingServices)
//   for (const service of missingServices) {
//     console.log("🚀 ~ updateMissingServices ~ service:", service)
//     const code = service.code;
//     console.log("🚀 ~ updateMissingServices ~ code:", code)
//     const desc = service.item;
//     console.log("🚀 ~ updateMissingServices ~ desc:", desc);
//     const index = record.item.findIndex(item => item === service.item);
//     console.log("🚀 ~ updateMissingServices ~ record.item:", record.item)
//     console.log("🚀 ~ updateMissingServices ~ index:", index)

//     const quantity = record.quantity[index];
//     console.log("🚀 ~ updateMissingSrugs ~ quantity:", quantity)
//     const cost = record.cost[index];
//     console.log("🚀 ~ updateMissingSrugs ~ cost:", cost)
//     const total = record.total[index];
//     console.log("🚀 ~ updateMissingSrugs ~ total:", total)
//     const awarded = record.awarded[index];
//     console.log("🚀 ~ updateMissingSrugs ~ awarded:", awarded)
//     const rejected = record.rejected[index];
//     console.log("🚀 ~ updateMissingSrugs ~ rejected:", rejected)
//     const rejectionReason = record.rejectionReasons[index];
//     console.log("🚀 ~ updateMissingSrugs ~ rejectionReason:", rejectionReason)
//     const quantityApproved = record.quantityApproved[index];
//     console.log("🚀 ~ updateMissingSrugs ~ quantityApproved:", quantityApproved)

//     const labQuery = `
//         SELECT tm_id, tm_tariff_id, tm_net_amt, sl_id, sl_cover_id, tm_cpt_code AS code, tm_description AS description, tm_currency AS currency,
//         (CASE
//             WHEN tm_cpt_code = '${code}' OR tm_pro_cpt_code = '${code}' THEN 'CODE'
//             WHEN (tm_pro_description LIKE '%${desc}%' OR tm_description = '%${desc}%') THEN 'DESCRIPTION'
//             ELSE 'UNKNOWN'
//         END) AS match_type  -- Detects how the item was matched
//         FROM tariff_master
//         JOIN provider_tariff ON pt_id = tm_tariff_id AND pt_status = '1'
//         JOIN service_list ON sl_id = tm_service_id
//         WHERE tm_cpt_code = '${code}' OR tm_pro_cpt_code = '${code}' OR (tm_pro_description LIKE '%${desc}%' OR tm_description = '%${desc}%' )
//         -- AND (pt_networkid = ? OR pt_networkid = 0)
//         -- AND (tm_claim_type = ? OR tm_claim_type = 0)
//         -- AND sl_ip_op != ?
//         LIMIT 1`;
//     console.log("Checking for lab item:", service);
//     const [serviceRecord] = await connection.query(labQuery);
//     console.log("🚀 ~ updateMissingServices ~ serviceRecord:", serviceRecord)

//     if (serviceRecord.length) {
//       await connection.query(
//         `INSERT INTO claim_details SET ?`,
//         {
//             cd_claim_id: claimData.claim_id,
//             cd_auth_id: 0,
//             cd_cover_id: serviceRecord[0].sl_cover_id,
//             cd_plan_id: 0,
//             cd_activity_type: serviceRecord[0].sl_id,
//             cd_tooth: '',
//             cd_activity_code: serviceRecord[0].code,
//             cd_activity_des: serviceRecord[0].description,
//             cd_activity_req_amt: total,
//             cd_discount_amt: 0,
//             cd_ded_copay_amt: 0,
//             cd_deduction_amt: total - awarded,
//             cd_qty_claimed: quantity,
//             cd_qty_approved: quantityApproved,
//             cd_claimed_amt: cost,
//             cd_approved_amt: awarded,
//             cd_recovery: 0,
//             cd_activity_net_amt: awarded,
//             cd_activity_remark: "",
//             cd_eye: null,
//             cd_cylinder: null,
//             cd_spine: null,
//             cd_access: null,
//             cd_pack_price: null,
//             cd_no_session: null,
//             // cd_auth_approval: cost - cost === 0 ? 1 : 0,
//             cd_auth_approval: rejectionReason !== 'Approved' ? 0 : 1,
//             cd_stay: null,
//             cd_risk_net_amt: awarded,
//             cd_is_skip: 0,
//             cd_den_code: null,
//             cd_tarif_id: serviceRecord[0].tm_tariff_id,
//             cd_exceed_amt: 0,
//             cd_vat_amt: 0,
//             cd_before_copay: 0,
//             cd_before_discount: 0,
//             cd_tm_id: serviceRecord[0].tm_id,
//             cd_pkg_code: null,
//             cd_payable_amt: cost,
//             cd_uncovered_amt: 0,
//             cd_perday_amt: 0,
//             cd_is_nonnetwork: 0,
//             cd_doctor_id: 0,
//             // cd_service_flag: 0,
//             cd_activity_req_qty: null,
//             cd_pkg_description: null,
//             cd_pkg_icu: null,
//             cd_pkg_ward: null,
//             cd_prov_net_amt: null,
//             cd_master_type: 1,
//             cd_den_code_des: null,
//             cd_add_den_code_desc: rejectionReason === 'Approved' ? '' : rejectionReason,
//             cd_service_date: null,
//             cd_activity_net_qty: null,
//             cd_net_amt_paid: null,
//             cd_extra_flag: null,
//             cd_tariff_amt: serviceRecord[0].tm_net_amt,
//             cd_currency: serviceRecord[0].currency,
//             cd_exchange_rate: 1,
//         }
//       );
//     } else {
//       await logNotFoundItems(notFoundItemsFile, { type: "service", item: service.item });
//     }
//   }
//   } catch (e) {
//     console.error("Error updating missing services", e);
//     throw e;
//   }
// }

async function logNotFoundItems(filePath, items) {
  try {
    let existingData = [];
    try {
      const data = await fs.readFile(filePath, "utf-8");
      existingData = data.trim() ? JSON.parse(data) : [];
      if (!Array.isArray(existingData)) {
        throw new Error("Invalid file format");
      }
    } catch (error) {
      console.warn(
        "Could not read existing not found items, initializing new file."
      );
      existingData = [];
    }

    existingData.push(...(Array.isArray(items) ? items : [items]));
    await fs.writeFile(
      filePath,
      JSON.stringify(existingData, null, 2),
      "utf-8"
    );
  } catch (error) {
    console.error("Error logging not found items:", error);
    throw error;
  }
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

// async function getPharmacyCoverId(connection, providerType, ipop, isChecker = 0, icdCodes = []) {
//     try {
//         console.log("🚀 ~ getPharmacyCoverId ~ icdCodes:", icdCodes)
//         console.log("🚀 ~ getPharmacyCoverId ~ isChecker:", isChecker)
//         console.log("🚀 ~ getPharmacyCoverId ~ ipop:", ipop)
//         console.log("🚀 ~ getPharmacyCoverId ~ providerType:", providerType)
//         if (!ipop || ipop === 0) ipop = 1;

//         let dental = 0;
//         let maternity = 0;
//         let coverId = 112;
//         let chk = 0;
//         let q1 = "";
//         let q2 = "";

//         if (icdCodes.length > 0) {
//             const icdCodeList = icdCodes.map(code => `'${code}'`).join(", ");

//             q1 = `
//                 SELECT * FROM icd_service_map
//                 JOIN covers ON c_id = cicm_cover_id
//                 WHERE (c_id = 114 OR c_type_tab = 4 OR c_cover_type_rule = 4)
//                 AND cicm_code IN (${icdCodeList})`;
//             const dentalResult = await connection.query(q1);
//             console.log("🚀 ~ getPharmacyCoverId ~ dentalResult:", dentalResult)
//             dental = dentalResult[0].length;

//             q2 = `
//                 SELECT * FROM icd_service_map
//                 JOIN covers ON c_id = cicm_cover_id
//                 WHERE (c_id = 113 OR c_type_tab = 3 OR c_cover_type_rule = 3)
//                 AND cicm_code IN (${icdCodeList})`;
//             const maternityResult = await connection.query(q2);
//             console.log("🚀 ~ getPharmacyCoverId ~ maternityResult:", maternityResult)
//             maternity = maternityResult[0].length;
//         }

//         if (maternity > 0) {
//             if (ipop === 1 && isChecker === 2) {
//                 coverId = 246; // Maternity + chronic + OP
//                 chk = 1;
//             } else if (ipop === 2) {
//                 coverId = 116; // IP maternity pharmacy cover id
//                 chk = 2;
//             } else if (ipop === 1) {
//                 coverId = 118; // OP maternity pharmacy cover id
//                 chk = 3;
//             }
//         } else if (dental > 0 || providerType === 6) {
//             coverId = 245; // If ICD is dental
//             chk = 4;
//         } else if (ipop === 1 && isChecker === 2) {
//             coverId = 247; // Chronic + OP
//             chk = 5;
//         } else if (ipop === 2) {
//             coverId = 110; // IP pharmacy cover id
//             chk = 6;
//         } else if (ipop === 1) {
//             coverId = 112; // OP pharmacy cover id
//             chk = 7;
//         }

//         console.log(`Cover ID: ${coverId}, chk: ${chk}, q1: ${q1},\n q2: ${q2}`);
//         return coverId;
//     } catch(e) {
//         throw new Error("Error while getting pharmacy cover id", e)
//     }
// }

// async function getUnitDrops(connection) {
//     try {
//         const unitDrops = await connection.query("SELECT pud_id, pud_unit, pud_value FROM pbm_unit_details");
//         let unitWiseDrops = {};

//         if (Array.isArray(unitDrops[0]) && unitDrops[0].length > 0) {
//             unitDrops[0].forEach(drop => {
//                 if (!unitWiseDrops[drop.pud_unit]) {
//                     unitWiseDrops[drop.pud_unit] = [];
//                 }
//                 unitWiseDrops[drop.pud_unit].push(drop);
//             });
//         }

//         return unitWiseDrops;
//     } catch (error) {
//         console.error("Error fetching unit drops:", error);
//         return {};
//     }
// }

async function getProviderInfo(connection, providerId) {
  const providerQuery = `
  SELECT provider_id, provider_name, provider_type 
  FROM providers 
  WHERE provider_name LIKE '%${providerId}%' OR provider_id = '${providerId}';
  `;

  // const likeValue = `%${providerId}%`; // Add wildcards around the providerId
  const [provider] = await connection.query(providerQuery);

  console.log("🚀 ~ getProviderInfo ~ provider:", provider);
  return provider;
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
  console.log("🚀 ~ updateClaimTotals ~ typeOfVisit:", typeOfVisit);
  console.log("🚀 ~ updateClaimTotals ~ dateOfAdmission:", dateOfAdmission);
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

  const [drugDetails] = await connection.query(
    `SELECT SUM(cdp_gross_amount) AS total_claimed, 
            SUM(cdp_approve_amt) AS total_approved, 
            SUM(cdp_deduction_amt) AS total_deduction,
            SUM(cdp_drug_price) as total_tariff
            FROM claim_drugs_prescribed WHERE cdp_claim_id = (SELECT claim_id FROM claim WHERE claim_id = ?)`,
    [claimId]
  );

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

async function findMemberInfo(connection, memberNumber) {
  try {
    memberNumber = String(memberNumber);
    const [member] = await connection.query(
      `
            SELECT ms_id, ms_tob_plan_id, ms_plan_network, mm_member_id, mm_member_id, ms_policy_id
            FROM members
            INNER JOIN members_schemes ms ON ms.ms_member_id = members.mm_id
            WHERE (mm_nin_number = ? OR mm_national_id = ? OR mm_member_id = ?) 
            ORDER BY ms_id DESC LIMIT 1`,
      [memberNumber, memberNumber, memberNumber]
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

const lotCache = new Map();
async function getOrCreateLotStatement(
  connection,
  memberInfo,
  record,
  claimRemark
) {
  //   console.log("🚀 ~ getOrCreateLotStatement ~ record:", record);

  // Use a key combining service provider and dateOfConsultation.
  const dateKey = record.dateOfConsultation.slice(0, 7); // e.g., "2024-12"
  const key = `${record.serviceProvider}|${dateKey}`;

  console.log("🚀 ~ getOrCreateLotStatement ~ key:", key);
  if (lotCache.has(key)) {
    console.log(`Reusing lot for ${key}: ${lotCache.get(key)}`);
    return lotCache.get(key);
  }

  // Assume maxLotNumber() returns an object with property max_lot_no.
  const { max_lot_no } = await maxLotNumber(connection);
  console.log("🚀 ~ getOrCreateLotStatement ~ max_lot_no:", max_lot_no);
  await createLotStatement(
    connection,
    memberInfo,
    max_lot_no,
    record,
    claimRemark
  );
  lotCache.set(key, max_lot_no);
  console.log(`Created new lot for ${key}: ${max_lot_no}`);
  return max_lot_no;
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

async function createLotStatement(
  connection,
  memberInfo,
  lot_no,
  record,
  claimRemark
) {
  // create new statement
  console.log("Creating new statement...");

  const provider = await getProviderInfo(connection, record.serviceProvider);
  try {
    const [result] = await connection.query("INSERT INTO lots SET ?", {
      lot_no: lot_no,
      lot_receive_date: new Date(record.dateOfConsultation)
        .toISOString()
        .slice(0, 10),
      lot_from_date: null,
      lot_to_date: null,
      lot_type: 1, // in patient or out patient
      lot_amount: record.claimed,
      lot_provider_id: provider[0]?.provider_id,
      lot_total_claim: 0,
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
  } catch (error) {
    throw new Error(`Failed to insert lot: ${error}`);
  }
}

async function logSuccessfulRecord(filePath, record) {
  try {
    console.log("Attempting to log successful record");
    // console.log('Attempting to log successful record:', record);
    const existingData = await fs.readFile(filePath, "utf-8");
    const records = existingData ? JSON.parse(existingData) : [];
    if (!Array.isArray(records)) {
      throw new Error("Invalid data format: expected an array");
    }
    const recordWithClaimId = { ...record };
    records.push(recordWithClaimId);
    await fs.writeFile(filePath, JSON.stringify(records, null, 2), "utf-8");
    console.log("Successfully logged record to:", filePath);
  } catch (error) {
    console.error("Error logging successful record:", error);
  }
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

module.exports = { updateClaims };
