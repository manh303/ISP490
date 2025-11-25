export const fmt=(n)=>new Intl.NumberFormat().format(n);

/**
 * Parse API error details from Pydantic validation errors
 * @param {Array} detail - Array of error objects from API response
 * @returns {Object} - Object with field names as keys and clean error messages as values
 */
export function parseApiErrors(detail) {
  const errors = {};
  detail.forEach(err => {
    const field = err.loc[err.loc.length - 1]; // e.g., "full_name", "phone"
    let msg = err.msg;
    if (err.type === 'value_error' && msg.startsWith('Value error, ')) {
      msg = msg.replace(/^Value error, /, '');
    }
    // For missing fields, keep the message as is or customize if needed
    if (err.type === 'missing') {
      msg = 'Trường bắt buộc'; // Vietnamese for "Field required"
    }
    errors[field] = msg;
  });
  return errors;
}