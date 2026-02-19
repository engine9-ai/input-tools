---
name: transaction-mapping
description: Assists in writing JavaScript mapping functions that map 3rd-party payment/transaction data into the standard Transaction schema (Frakture Transactions Data). Use when mapping Stripe, PayPal, donor databases, CSV exports, or any external payment data into engine9 transaction records.
---

# Transaction Mapping

Use this skill when writing a JavaScript function that maps 3rd-party payment or donation data into the standard Transaction schema. The canonical schema is defined in [Frakture Transactions Data](https://frakture.notion.site/Frakture-Transactions-Data-442349ac436a4f7db8e7d732359e7d8f). The codebase implements this in `interfaces/transaction/schema.js` and processes mapped rows via `interfaces/transaction/transforms/inbound/upsert_tables.js`.

## Mapping workflow

1. **Inspect the source** – Identify which 3rd-party fields correspond to Transaction schema fields (amount, date, person identifier, recurring vs one-time, refunds, etc.).
2. **Implement a pure mapping function** – `(rawRow) => transactionRow`. Do not set `id` or `entry_type_id` in the mapper; the inbound transform derives them from `getEntryTypeId` and `getTimelineEntryUUID`.
3. **Return a single object per payment** – One 3rd-party record (or one logical payment) → one Transaction-shaped object. For refunds that are separate rows, map each and use `entry_type: 'TRANSACTION_REFUND'` and `refund_amount` where appropriate.
4. **Validate required fields** – Ensure every returned object has `ts`, `amount`, `remote_person_id`, and either `entry_type` or `entry_type_id`. The pipeline will throw if any of these are missing.

## Required fields (must be present on every mapped row)

| Field              | Type   | Notes                                                                                                                                              |
| ------------------ | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ts`               | Date   | Transaction date/time. Accept ISO string or number (ms); converted with `new Date(ts)`.                                                            |
| `amount`           | number | Transaction amount (currency units).                                                                                                               |
| `remote_person_id` | string | Payer/donor identifier from the 3rd-party source (e.g. donor_id, customer_id). The pipeline resolves this to the in-house `person_id`.             |
| `entry_type`       | string | Or `entry_type_id`. Use a value from [Transaction entry types](#transaction-entry-types) (e.g. `'TRANSACTION_ONE_TIME'`, `'TRANSACTION_INITIAL'`). |

**Note:** Do not set `input_id` in the mapper. It is calculated by the pipeline from `remote_page_name` or `remote_input_id`.

## Optional but important fields

| Field                                                               | Type   | Notes                                                                                                                                 |
| ------------------------------------------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `refund_amount`                                                     | number | For refunds; use with `entry_type: 'TRANSACTION_REFUND'` when applicable.                                                             |
| `remote_transaction_id`                                             | string | Idempotency key from 3rd-party system; used for upserts and UUID derivation.                                                          |
| `remote_page_name`                                                  | string | e.g. campaign or form name; used (with `remote_input_id`) to derive `input_id`.                                                       |
| `remote_recurring_id`                                               | string | External subscription/recurring series id.                                                                                            |
| `recurs`                                                            | string | Frequency: `'daily'`, `'weekly'`, `'monthly'`, `'quarterly'`, `'annually'`, `'semi-annually'`. Inbound transform maps to `recurs_id`. |
| `recurring_number`                                                  | number | Occurrence index in a series (e.g. 1st, 2nd donation in a recurring set).                                                             |
| `given_name`, `family_name`, `email`                                | string | Payer/donor info; useful for matching or display.                                                                                     |
| `source_code_id`, `override_source_code_id`, `final_source_code_id` | number | Attribution/source.                                                                                                                   |
| `recommended_message_id`, `override_message_id`, `final_message_id` | UUID   | Message attribution.                                                                                                                  |
| `extra`                                                             | object | JSON blob for vendor-specific data that doesn’t fit the schema.                                                                       |
| `remote_entry_uuid`                                                 | UUID   | If the 3rd-party system provides a stable UUID, set this and it will be used as the record id.                                        |

## Transaction entry types

Use exactly these `entry_type` string values (or the numeric `entry_type_id`). Import `TIMELINE_ENTRY_TYPES` from `@engine9/input-tools` for the full map.

| entry_type               | entry_type_id | Use when                                                     |
| ------------------------ | ------------- | ------------------------------------------------------------ |
| `TRANSACTION`            | 10            | Generic; prefer a more specific type when known.             |
| `TRANSACTION_ONE_TIME`   | 11            | Single, non-recurring payment.                               |
| `TRANSACTION_INITIAL`    | 12            | First payment in a recurring series.                         |
| `TRANSACTION_SUBSEQUENT` | 13            | Later payment in a recurring series.                         |
| `TRANSACTION_RECURRING`  | 14            | Recurring payment, order unknown.                            |
| `TRANSACTION_REFUND`     | 15            | Refund; set `refund_amount` and optionally link to original. |

## Mapping function template

```javascript
/**
 * Maps a single 3rd-party payment record to the Transaction schema.
 * @param {Object} row - Raw record from the payment system (e.g. Stripe charge, CSV row).
 * @returns {Object} Transaction-shaped object (required: ts, amount, remote_person_id, entry_type).
 */
function mapPaymentToTransaction(row) {
  return {
    ts: row.created_at ?? row.date ?? row.timestamp,
    amount: parseFloat(row.amount ?? row.total ?? 0),
    remote_person_id: row.donor_id ?? row.customer_id ?? row.external_id,
    entry_type: row.recurring ? 'TRANSACTION_SUBSEQUENT' : 'TRANSACTION_ONE_TIME',
    remote_transaction_id: row.id ?? row.transaction_id,
    remote_page_name: row.campaign ?? row.form_name ?? null,
    email: row.email ?? null,
    given_name: row.first_name ?? null,
    family_name: row.last_name ?? null,
    remote_recurring_id: row.subscription_id ?? null,
    recurs: row.interval === 'month' ? 'monthly' : null,
    recurring_number: row.occurrence ?? null,
    extra: row.raw ? { raw: row.raw } : undefined
  };
}
```

- **Do not** set `id`, `entry_type_id`, or `input_id` in the mapper; the inbound transform/pipeline sets them (e.g. `input_id` from `remote_page_name` or `remote_input_id`).
- **Do** normalize dates to something `new Date(ts)` can parse (ISO string or ms).
- **Do** use `remote_person_id` for the 3rd-party payer/donor identifier; the pipeline resolves it to in-house `person_id`.
- **Do** use `remote_transaction_id` when the source has a stable id for idempotent upserts.
- **Do** use `entry_type` (string) unless you have a reason to use numeric `entry_type_id`.

## Refunds

- If the source has a separate refund row: set `entry_type: 'TRANSACTION_REFUND'`, `refund_amount`, and `ts`; keep `amount` as 0 or the original amount depending on product rules.
- If the source only has a flag: you may emit one row with `amount` and optionally `refund_amount`, and still use `TRANSACTION_REFUND` for the entry_type when it’s a refund.

## Validation

- After writing the mapper, verify that sample rows include `ts`, `amount`, `remote_person_id`, and `entry_type` (or `entry_type_id`). The pipeline will throw on the first row missing any of these.
- Ensure `remote_person_id` is the identifier from the 3rd-party source (e.g. donor_id, customer_id); the pipeline resolves it to the in-house `person_id`.

## Additional reference

- Full schema and column types: [reference.md](reference.md)
- Schema in code: `interfaces/transaction/schema.js`
- Inbound transform (sets `id`, `entry_type_id`, `recurs_id`): `interfaces/transaction/transforms/inbound/upsert_tables.js`
- Entry type constants: `input-tools/timelineTypes.js` (`TIMELINE_ENTRY_TYPES`)
