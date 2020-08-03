from typing import Dict

from ..model.etl_stats import EtlTransformation


class SessionTracker:
    """
    Keeps track which SQLAlchemy sessions should be inspected for
    storing ETLTransformation statistics.
    """

    sessions: Dict[int, EtlTransformation] = {}

    @staticmethod
    def remove_session(session_id) -> None:
        """
        Remove item from sessions, so ETL statistics can no longer be
        tracked for it.

        :param session_id: int
             id value of SQLAlchemy session object
        :return: None
        """
        if session_id in SessionTracker.sessions:
            del SessionTracker.sessions[session_id]
