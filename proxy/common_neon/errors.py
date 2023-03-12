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
    def __init__(self):
        super().__init__('The Solana node is not synchronized with a Solana cluster')


class SolanaUnavailableError(RuntimeError):
    def __init__(self):
        super().__init__('The Solana node is unavailable')


class NonceTooLowError(RuntimeError):
    pass


class NoMoreRetriesError(RuntimeError):
    def __init__(self):
        super().__init__('The transaction is too complicated. No more retries to complete the Neon transaction')


class CUBudgetExceededError(RuntimeError):
    def __init__(self):
        super().__init__('The transaction is too complicated. Solana`s computing budget is exceeded')


class InvalidIxDataError(RuntimeError):
    def __init__(self):
        super().__init__('Wrong instruction data')


class RequireResizeIterError(RuntimeError):
    def __init__(self):
        super().__init__('Transaction requires resize iterations')


class BlockHashNotFound(RuntimeError):
    def __init__(self):
        super().__init__('Blockhash not found')
