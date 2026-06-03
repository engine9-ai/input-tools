const SOURCE_CODE_OVERRIDE = 0;
const CRM_ORIGIN = 1;
const ACQUISITION = 2;
// Generic signup
const SIGNUP = 3;
// Known first time signup
const SIGNUP_INITIAL = 4;
// Known subsequent signup
const SIGNUP_SUBSEQUENT = 5;
const UNSUBSCRIBE = 6;
// Generic monetary transaction
const TRANSACTION = 10;
// known one-time transaction
const TRANSACTION_ONE_TIME = 11;
// known initial recurring transaction
const TRANSACTION_INITIAL = 12;
// known initial transaction
const TRANSACTION_SUBSEQUENT = 13;
// known recurring, not sure if first
const TRANSACTION_RECURRING = 14;
// refunded transaction, first instance
const TRANSACTION_REFUND = 15;
const SEGMENT_PERSON_ADD = 16;
const SEGMENT_PERSON_REMOVE = 17;
//This is useful to have a reference of someone just being in a segment
//not necessarily their addition or removal
const SEGMENT_PERSON_IN_SEGMENT = 18;
// unknown generic conversion on a message
const MESSAGE_CONVERSION = 20;
// advocacy conversion on a message
const MESSAGE_CONVERSION_ADVOCACY = 21;
// unknown transaction conversion on a message
const MESSAGE_CONVERSION_TRANSACTION = 22;
const MESSAGE_DELIVERY_FAILURE_SHOULD_RETRY = 25;
const MESSAGE_DELIVERY_FAILURE_SHOULD_NOT_RETRY = 26;
const SMS_SEND = 30;
const SMS_DELIVERED = 31;
const SMS_CLICK = 33;
const SMS_UNSUBSCRIBE = 34;
const SMS_BOUNCE = 37;
const SMS_SPAM = 38;
const SMS_REPLY = 39;
const EMAIL_SEND = 40;
const EMAIL_DELIVERED = 41;
const EMAIL_OPEN = 42;
const EMAIL_CLICK = 43;
const EMAIL_UNSUBSCRIBE = 44;
const EMAIL_SOFT_BOUNCE = 45;
const EMAIL_HARD_BOUNCE = 46;
const EMAIL_BOUNCE = 47;
const EMAIL_SPAM = 48;
const EMAIL_REPLY = 49;
const PHONE_CALL_ATTEMPT = 50;
const PHONE_CALL_SUCCESS = 51;
const PHONE_CALL_FAIL = 52;
// Generic action
const FORM_SUBMIT = 60;
const FORM_PETITION = 61;
const FORM_PETITION_CONTACT_TARGET = 62;
const FORM_ADVOCACY = 66;
const FORM_SURVEY = 67;
const FILE_IMPORT = 70;
// For tracking exports or data pushes
// Generic export of data
const EXPORT = 80;
// Export specifically for pushing data to remote systems
const EXPORT_FOR_REMOTE = 81;
// Export specifically for sending out messages
const EXPORT_FOR_MESSAGING = 82;
//These are for actions we may not have exact
// details on, but are useful for modeling, etc
const INFERRED_ENTRY = 91;
const INFERRED_MESSAGE = 92;
const INFERRED_FORM = 93;
// DO. NOT. CHANGE. (once finalized)
// should probably have offsets between types
// ie email, transaction, etc.
// or maybe top level TIMELINE_ROW_TYPES and subtypes. /shrug emoji
const TIMELINE_ENTRY_TYPES = {
  SOURCE_CODE_OVERRIDE,
  CRM_ORIGIN,
  ACQUISITION,
  // signups
  SIGNUP,
  SIGNUP_INITIAL,
  SIGNUP_SUBSEQUENT,
  UNSUBSCRIBE,
  TRANSACTION,
  TRANSACTION_INITIAL,
  TRANSACTION_SUBSEQUENT,
  TRANSACTION_ONE_TIME,
  TRANSACTION_RECURRING,
  TRANSACTION_REFUND,
  SEGMENT_PERSON_ADD,
  SEGMENT_PERSON_REMOVE,
  SEGMENT_PERSON_IN_SEGMENT,
  SMS_SEND,
  SMS_DELIVERED,
  SMS_CLICK,
  SMS_UNSUBSCRIBE,
  SMS_BOUNCE,
  SMS_SPAM,
  SMS_REPLY,
  // email interactions
  EMAIL_SEND,
  EMAIL_DELIVERED,
  EMAIL_OPEN,
  EMAIL_CLICK,
  EMAIL_UNSUBSCRIBE,
  EMAIL_SOFT_BOUNCE,
  EMAIL_HARD_BOUNCE,
  EMAIL_BOUNCE,
  EMAIL_REPLY,
  EMAIL_SPAM,
  PHONE_CALL_ATTEMPT,
  PHONE_CALL_SUCCESS,
  PHONE_CALL_FAIL,
  // forms
  FORM_SUBMIT,
  FORM_PETITION,
  FORM_PETITION_CONTACT_TARGET,
  FORM_ADVOCACY,
  FORM_SURVEY,
  MESSAGE_CONVERSION,
  MESSAGE_CONVERSION_ADVOCACY,
  MESSAGE_CONVERSION_TRANSACTION,
  MESSAGE_DELIVERY_FAILURE_SHOULD_RETRY,
  MESSAGE_DELIVERY_FAILURE_SHOULD_NOT_RETRY,
  FILE_IMPORT,
  EXPORT,
  EXPORT_FOR_REMOTE,
  EXPORT_FOR_MESSAGING,
  INFERRED_ENTRY,
  INFERRED_MESSAGE,
  INFERRED_FORM
};
Object.entries(TIMELINE_ENTRY_TYPES).forEach(([k, v]) => {
  TIMELINE_ENTRY_TYPES[v] = k;
});
export { TIMELINE_ENTRY_TYPES };
export default {
  TIMELINE_ENTRY_TYPES
};
