'use strict';
// Retry the SAME EIP-712 message/intent after a lost acknowledgement, never a new nonce.
function createCloseSender({ master, takeNonce, deadline, sign, post }) {
  return async function close(positionIds, { request = null, onPrepared = () => {} } = {}) {
    const ids = positionIds.map(String);
    const message = request || { user: master, positionIds: ids, nonce: takeNonce(), deadline: deadline() };
    if (String(message.user).toLowerCase() !== master.toLowerCase() || JSON.stringify(message.positionIds.map(String)) !== JSON.stringify(ids)) throw new Error('Close request identity mismatch');
    const { signed, intentId } = await sign(message);
    // Synchronous durable state callback; no HTTP if this fails.
    onPrepared({ message, intentId });
    const result = await post({ action: 'closePositions', signed });
    if (result.data.intentId && result.data.intentId.toLowerCase() !== intentId.toLowerCase()) throw new Error('Close intent acknowledgement mismatch');
    return { ...result, intentId, request: message };
  };
}
module.exports = { createCloseSender };
