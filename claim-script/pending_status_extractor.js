// extractor.js
const fs = require('fs');
const path = require('path');

const INPUT_FILE = path.join(__dirname, 'gen-claims/output/data.json');
const PENDING_FILE = path.join(__dirname, 'gen-claims/output/data_transformed_pending.json');
const REMAINING_FILE = path.join(__dirname, 'gen-claims/output/data_transformed.json');

try {
  // 1. Read & parse input (expects an array of objects)
  const raw = fs.readFileSync(INPUT_FILE, 'utf8');
  const allRecords = JSON.parse(raw);

  if (!Array.isArray(allRecords)) {
    throw new Error('Expected data.json to contain a JSON array of objects.');
  }

  // 2. Partition into “pending” vs. “remaining”
  const pending   = [];
  const remaining = [];

  allRecords.forEach((record) => {
    const { auditStatus, diagnosis } = record;

    // “pending” only if auditStatus is 'pending' AND diagnosis is empty-array or empty-string
    const isAuditPending   = auditStatus === 'pending';
    const isEmptyArray     = Array.isArray(diagnosis) && diagnosis.length === 0;
    const isEmptyString    = diagnosis === '';

    // if (isAuditPending && (isEmptyArray || isEmptyString)) {
    if (isEmptyArray || isEmptyString) {
      pending.push(record);
    } else {
      remaining.push(record);
    }
  });

  // 3. Write out each group to its own file
  fs.writeFileSync(
    PENDING_FILE,
    JSON.stringify(pending, null, 2),
    'utf8'
  );

  fs.writeFileSync(
    REMAINING_FILE,
    JSON.stringify(remaining, null, 2),
    'utf8'
  );

  console.log(`✅ Extracted ${pending.length} record(s) → ${path.basename(PENDING_FILE)}`);
  console.log(`✅ Wrote ${remaining.length} record(s) → ${path.basename(REMAINING_FILE)}`);
} catch (err) {
  console.error('Error:', err.message);
  process.exit(1);
}
