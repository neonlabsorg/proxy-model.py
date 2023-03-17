from __future__ import annotations


class EthereumError(Exception):
    def __init__(self, message: str, code=-32000, data=None):
        self.code = code
        self.message = message
        self.data = data

    def get_error(self):
        error = {'code': self.code, 'message': self.message}
        if self.data:
            error['data'] = self.data
        return error


class InvalidParamError(EthereumError):
    def __init__(self, message, data=None):
        EthereumError.__init__(self, message=message, code=-32602, data=data)


class ALTError(RuntimeError):
    pass


class BadResourceError(RuntimeError):
    pass


class BlockedAccountsError(RuntimeError):
    pass


class NodeBehindError(RuntimeError):
    def __init__(self, slots_behind: int):
        super().__init__(f'The Solana node is behind by {slots_behind} from the Solana cluster')


class SolanaUnavailableError(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


class NonceTooLowError(RuntimeError):
    def __init__(self, sender_address: str, tx_nonce: int, state_tx_cnt: int):
        super().__init__(
            f'nonce too low: address {sender_address}, '
            f'tx: {tx_nonce} state: {state_tx_cnt}'
        )
        self._sender_address = sender_address
        self._tx_nonce = tx_nonce
        self._state_tx_cnt = state_tx_cnt

    def clone(self, sender_address) -> NonceTooLowError:
        return NonceTooLowError(sender_address, self._tx_nonce, self._state_tx_cnt)


class NoMoreRetriesError(RuntimeError):
    def __init__(self):
        super().__init__('The Neon transaction is too complicated. No more retries to complete the Neon transaction')


class WrongNumberOfItersError(RuntimeError):
    def __init__(self):
        super().__init__('Wrong number of iterations of the Neon transaction')


class CUBudgetExceededError(RuntimeError):
    def __init__(self):
        super().__init__('The Neon transaction is too complicated. Solana`s computing budget is exceeded')


class InvalidIxDataError(RuntimeError):
    def __init__(self):
        super().__init__('Wrong instruction data')


class RequireResizeIterError(RuntimeError):
    def __init__(self):
        super().__init__('Transaction requires resize iterations')


class BlockHashNotFound(RuntimeError):
    def __init__(self):
        super().__init__('Blockhash not found')
