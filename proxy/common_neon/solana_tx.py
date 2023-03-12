from __future__ import annotations

from typing import Sequence, Optional, Union, Dict, Any, Set

import abc

import solana.transaction
import solana.rpc.commitment

import solders.hash
import solders.keypair
import solders.pubkey
import solders.instruction
import solders.signature

SolTxIx = solders.instruction.Instruction
SolAccountMeta = solana.transaction.AccountMeta
SolBlockHash = solders.hash.Hash
SolAccount = solders.keypair.Keypair
SolSignature = solders.signature.Signature
SolPubKey = solders.pubkey.Pubkey
SolTxReceipt = Dict[str, Any]


class Commitment:
    Type = solana.rpc.commitment.Commitment

    NotProcessed = solana.rpc.commitment.Commitment('not-processed')
    Processed = solana.rpc.commitment.Processed
    Confirmed = solana.rpc.commitment.Confirmed
    Safe = solana.rpc.commitment.Commitment('safe')  # optimistic-finalized => 2/3 of validators
    Finalized = solana.rpc.commitment.Finalized

    Order = [NotProcessed, Processed, Confirmed, Safe, Finalized]

    @staticmethod
    def level(commitment: Type) -> int:
        for index, value in enumerate(Commitment.Order):
            if value == commitment:
                return index

        assert False, 'Wrong commitment'

    @staticmethod
    def upper_set(commitment: Type) -> Set[Type]:
        level = Commitment.level(commitment)
        return set(Commitment.Order[level:])

    @staticmethod
    def lower_set(commitment: Type) -> Set[Type]:
        level = Commitment.level(commitment)
        return set(Commitment.Order[:level])

    @staticmethod
    def to_solana(commitment: Type) -> Type:
        if commitment == Commitment.NotProcessed:
            return Commitment.Processed
        elif commitment == Commitment.Safe:
            return Commitment.Confirmed
        elif commitment in {Commitment.Processed, Commitment.Confirmed, Commitment.Finalized}:
            return commitment

        assert False, 'Wrong commitment'


class SolTxSizeError(RuntimeError):
    pass


class SolTx(abc.ABC):
    _empty_block_hash = SolBlockHash.default()

    def __init__(self, name: str = '', instructions: Optional[Sequence[SolTxIx]] = None):
        self._name = name
        self._tx = solana.transaction.Transaction(instructions=instructions)
        self._is_signed = False

    @property
    def name(self) -> str:
        return self._name

    def is_empty(self) -> bool:
        return len(self._tx.instructions) == 0

    @property
    def recent_block_hash(self) -> Optional[SolBlockHash]:
        block_hash = self._tx.recent_blockhash
        if block_hash == self._empty_block_hash:
            return None
        return block_hash

    @recent_block_hash.setter
    def recent_block_hash(self, block_hash: Optional[SolBlockHash]) -> None:
        self._tx.recent_blockhash = block_hash
        self._is_signed = False

    def add(self, *args: Union[SolTx, SolTxIx]) -> SolTx:
        ix_list = list(self._tx.instructions)
        for arg in args:
            if isinstance(arg, SolTxIx):
                ix_list.append(arg)
            elif isinstance(arg, SolTx):
                ix_list.extend(arg._tx.instructions)
            else:
                raise ValueError('invalid instruction:', arg)

        self._tx.instructions = ix_list
        self._is_signed = False
        return self

    def serialize(self) -> bytes:
        assert self._is_signed, 'transaction has not been signed'
        result = self._serialize()
        if len(result) > solana.transaction.PACKET_DATA_SIZE:
            raise SolTxSizeError('Transaction too big')
        return result

    def sign(self, signer: SolAccount) -> None:
        self._sign(signer)
        self._is_signed = True

    @property
    def is_signed(self) -> bool:
        return self._is_signed

    @property
    def signature(self) -> SolSignature:
        assert self._is_signed, 'transaction has not been signed'
        return self._signature()

    @abc.abstractmethod
    def _serialize(self) -> bytes:
        pass

    @abc.abstractmethod
    def _sign(self, signer: SolAccount) -> None:
        pass

    @abc.abstractmethod
    def _signature(self) -> SolSignature:
        pass
