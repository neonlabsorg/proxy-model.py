import logging

from ..common_neon.solana_tx import SolCommit
from ..common_neon.config import Config
from ..common_neon.solana_interactor import SolInteractor


LOG = logging.getLogger(__name__)


def get_start_slot(config: Config, solana: SolInteractor, last_known_slot: int) -> int:
    latest_slot = solana.get_block_slot(SolCommit.Finalized)
    start_slot = _get_start_slot_from_config(config, last_known_slot, latest_slot)

    first_slot = get_first_slot(solana)
    start_slot = max(start_slot, first_slot)
    LOG.info(f'FIRST_AVAILABLE_SLOT={first_slot}: started the receipt slot from {start_slot}')
    return start_slot


def get_first_slot(solana: SolInteractor) -> int:
    first_slot = solana.get_first_available_block()
    if first_slot > 0:
        first_slot += 512
    return first_slot


def _get_start_slot_from_config(config: Config, last_known_slot: int, latest_slot: int) -> int:
    """
    This function allow to skip some part of history.
    - LATEST - start from the last block slot from Solana
    - CONTINUE - continue from the last parsed slot of from latest
    - NUMBER - first start from the number, then continue from last parsed slot
    """
    last_known_slot = 0 if not isinstance(last_known_slot, int) else last_known_slot
    start_int_slot = 0

    start_slot = config.start_slot.upper().strip()
    LOG.info(f'Starting the receipt slot with LAST_KNOWN_SLOT={last_known_slot} and START_SLOT={start_slot}')

    if start_slot not in {'CONTINUE', 'LATEST'}:
        try:
            start_int_slot = min(int(start_slot), latest_slot)
        except (Exception,):
            LOG.error(f'Wrong value START_SLOT={start_slot}: use START_SLOT=0')
            start_int_slot = 0

    if start_slot == 'CONTINUE':
        if last_known_slot > 0:
            LOG.info(f'START_SLOT={start_slot}: started the receipt slot from previous run {last_known_slot}')
            return last_known_slot
        else:
            LOG.info(f'START_SLOT={start_slot}: forced the receipt slot from the latest Solana slot')
            start_slot = 'LATEST'

    if start_slot == 'LATEST':
        LOG.info(f'START_SLOT={start_slot}: started the receipt slot from the latest Solana slot {latest_slot}')
        return latest_slot

    if start_int_slot < last_known_slot:
        LOG.info(f'START_SLOT={start_slot}: started the receipt slot from previous run {last_known_slot}')
        return last_known_slot

    LOG.info(f'START_SLOT={start_slot}: started the receipt slot from {start_int_slot}')
    return start_int_slot
