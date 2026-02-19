# Transaction schema reference

Canonical documentation: [Frakture Transactions Data](https://frakture.notion.site/Frakture-Transactions-Data-442349ac436a4f7db8e7d732359e7d8f).

This file summarizes the schema as implemented in `interfaces/transaction/schema.js` and how the inbound transform and input-tools use it.

## Table: transaction

| Column                  | DB type        | Required for mapping?     | Notes                                                                                                            |
| ----------------------- | -------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| id                      | id_uuid        | No (set by transform)     | Set by `getTimelineEntryUUID` in inbound transform.                                                              |
| ts                      | datetime       | **Yes**                   | Transaction date/time.                                                                                           |
| input_id                | uuid           | No (calculated)           | Derived by pipeline from `remote_page_name` or `remote_input_id`; do not set in mapper.                          |
| entry_type_id           | int            | **Yes** (or entry_type)   | Set by transform from `entry_type` string if not provided.                                                       |
| person_id               | person_id      | No (resolved by pipeline) | In-house person id; resolved from `remote_person_id` by the pipeline.                                            |
| remote_person_id        | string         | **Yes**                   | Payer/donor identifier from 3rd-party source; pipeline resolves to `person_id`.                                  |
| amount                  | currency       | **Yes**                   | Transaction amount (currency units).                                                                             |
| remote_transaction_id   | string         | No                        | External id; used for upserts and id derivation.                                                                 |
| remote_page_name        | string         | No                        | e.g. campaign/form name; used (with remote_input_id) to derive input_id.                                         |
| remote_recurring_id     | string         | No                        | External recurring/subscription id.                                                                              |
| recurs_id               | int            | No                        | Set by transform from `recurs` (daily=1, weekly=2, monthly=3, quarterly=4, annually=5, semi-annually=6, else 0). |
| recurring_number        | int            | No                        | Occurrence index in series.                                                                                      |
| refund_amount           | currency       | No                        | For refunds.                                                                                                     |
| given_name              | string         | No                        | Payer first name.                                                                                                |
| family_name             | string         | No                        | Payer last name.                                                                                                 |
| email                   | string         | No                        | Payer email.                                                                                                     |
| source_code_id          | source_code_id | No                        | Attribution.                                                                                                     |
| override_source_code_id | source_code_id | No                        |                                                                                                                  |
| final_source_code_id    | source_code_id | No                        |                                                                                                                  |
| recommended_message_id  | uuid           | No                        |                                                                                                                  |
| override_message_id     | uuid           | No                        |                                                                                                                  |
| final_message_id        | uuid           | No                        |                                                                                                                  |
| extra                   | json           | No                        | Arbitrary vendor-specific data.                                                                                  |

## recurs → recurs_id (inbound transform)

The transform maps `recurs` (string) to `recurs_id` (int) when not already set:

| recurs                        | recurs_id |
| ----------------------------- | --------- |
| 'daily'                       | 1         |
| 'weekly'                      | 2         |
| 'monthly'                     | 3         |
| 'quarterly'                   | 4         |
| 'annually'                    | 5         |
| 'semi-annually'               | 6         |
| (none) + recurring_number > 1 | 3         |
| (none)                        | 0         |

## Transaction entry_type → entry_type_id

From `input-tools/timelineTypes.js`:

| entry_type             | entry_type_id |
| ---------------------- | ------------- |
| TRANSACTION            | 10            |
| TRANSACTION_ONE_TIME   | 11            |
| TRANSACTION_INITIAL    | 12            |
| TRANSACTION_SUBSEQUENT | 13            |
| TRANSACTION_RECURRING  | 14            |
| TRANSACTION_REFUND     | 15            |

## Required fields from the mapper

The mapper must provide (the pipeline derives or resolves the rest):

- `ts`
- `amount`
- `remote_person_id` (3rd-party payer/donor identifier; pipeline resolves to `person_id`)
- `entry_type` or `entry_type_id`

The pipeline calculates `input_id` from `remote_page_name` or `remote_input_id`. The inbound transform uses `input_id` and `person_id` (resolved from `remote_person_id`) with `getTimelineEntryUUID` to set `id`. If `remote_entry_uuid` is set and valid, it is used as `id` and the composite is not needed.
