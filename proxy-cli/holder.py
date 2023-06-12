from __future__ import annotations

import sys
from typing import List, Optional

from proxy.common_neon.solana_interactor import SolInteractor
from proxy.common_neon.operator_secret_mng import OpSecretMng
from proxy.common_neon.operator_resource_info import OpResIdentListBuilder, OpResIdent, OpResInfo
from proxy.common_neon.neon_tx_stages import NeonTxStage, NeonCreateHolderAccountStage, NeonDeleteHolderAccountStage
from proxy.common_neon.solana_tx_list_sender import SolTxListSender
from proxy.common_neon.neon_instruction import NeonIxBuilder
from proxy.common_neon.constants import FINALIZED_HOLDER_TAG, HOLDER_TAG
from proxy.common_neon.config import Config


class HolderHandler:
    def __init__(self):
        self._config = Config()
        self._solana = SolInteractor(self._config, self._config.solana_url)
        self.command = 'holder'

    def _get_res_ident_list(self) -> List[OpResIdent]:
        secret_list = OpSecretMng(self._config).read_secret_list()
        return OpResIdentListBuilder(self._config).build_resource_list(secret_list)

    @staticmethod
    def init_args_parser(parsers) -> HolderHandler:
        h = HolderHandler()
        h.root_parser = parsers.add_parser(h.command)
        h.sub_parser = h.root_parser.add_subparsers(title='command', dest='subcommand', description='valid commands')
        h.holder_parser = h.sub_parser.add_parser('list')
        h.create_parser = h.sub_parser.add_parser('create')
        h.create_parser.add_argument('holder-id', type=int, nargs=1, help='id of the holder account')
        h.create_parser.add_argument('operator-key', type=str, nargs=1, help='operator public key')
        h.delete_parser = h.sub_parser.add_parser('delete')
        h.delete_parser.add_argument('holder-id', type=int, nargs=1, help='id of the holder account')
        h.delete_parser.add_argument('operator-key', type=str, nargs=1, help='operator public key')
        return h

    def execute(self, args) -> None:
        if args.subcommand == 'create':
            self._create_holder_account(args)
        elif args.subcommand == 'delete':
            self._delete_holder_account(args)
        else:
            print(f'Unknown command {args.subcommand} for account', file=sys.stderr)
            return

    def _create_holder_account(self, args) -> None:
        op_res_info = self._find_op_res_info(args.operator_key, args.holder_id)
        if op_res_info is None:
            return

        holder_info = self._solana.get_holder_account_info(op_res_info.holder_account)
        if holder_info is not None:
            print(f'Holder account {args.operator_key}:{args.holder_id} already exist', file=sys.stderr)
            return

        size = self._config.holder_size
        balance = self._solana.get_multiple_rent_exempt_balances_for_size([size])[0]
        builder = NeonIxBuilder(self._config, op_res_info.public_key)
        stage = NeonCreateHolderAccountStage(builder, op_res_info.holder_seed, size, balance)
        self._execute_stage(stage, op_res_info)
        print(f'Holder account {args.operator_key}:{args.holder_id} is successfully created', file=sys.stderr)

    def _delete_holder_account(self, args) -> None:
        op_res_info = self._find_op_res_info(args.operator_key, args.holder_id)
        if op_res_info is None:
            return

        holder_info = self._solana.get_holder_account_info(op_res_info.holder_account)
        if holder_info is None:
            print(f'Holder account {op_res_info.holder_account} does not exist', file=sys.stderr)
            return

        if holder_info.tag not in {FINALIZED_HOLDER_TAG, HOLDER_TAG}:
            print(f'Holder account {args.operator_key}:{args.holder_id} has wrong tag', file=sys.stderr)
            return

        builder = NeonIxBuilder(self._config, op_res_info.public_key)
        stage = NeonDeleteHolderAccountStage(builder, op_res_info.holder_seed)
        self._execute_stage(stage, op_res_info)
        print(f'Holder account {args.operator_key}:{args.holder_id} is successfully deleted', file=sys.stderr)

    def _find_op_res_info(self, operator_key: str, holder_id: int) -> Optional[OpResInfo]:
        op_res_ident_list = self._get_res_ident_list()
        for op_res_ident in op_res_ident_list:
            if op_res_ident.res_id == holder_id and op_res_ident.public_key == operator_key:
                return OpResInfo.from_ident(op_res_ident)
        print(f'Unknown operator resource {operator_key}:{holder_id}', file=sys.stderr)
        return None

    def _execute_stage(self, stage: NeonTxStage, op_res_info: OpResInfo) -> None:
        stage.build()
        tx_sender = SolTxListSender(self._config, self._solana, op_res_info.signer)
        tx_sender.send([stage.tx])
