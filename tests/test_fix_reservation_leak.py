"""
REGRESSION TEST SUITE: Reservation Leak Prevention on Partial Allocation
Verifies Defect 4 from audit/POST_REPAIR_AUDIT.md:
Unclaimed agent reservations are immediately released back to AVAILABLE
when borrower pool is exhausted or allocation fails partially.
"""

import uuid
import pytest
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from smartdialer.dialer.progressive import ProgressiveDialer
from smartdialer.dialer.predictive import PredictiveDialer
from smartdialer.models.agent import Agent
from smartdialer.models.borrower import Borrower
from smartdialer.models.campaign import Campaign
from smartdialer.models.enums import AgentState, BorrowerState, CampaignMode
from smartdialer.safety.safety_controller import SafetyController, DialingRequest


def test_progressive_partial_borrower_exhaustion_cleans_up_agents(sync_test_engine):
    """
    When ProgressiveDialer claims 4 agents via SafetyController, but only 1 borrower is eligible,
    the 3 unallocated agents MUST be immediately transitioned back to AVAILABLE in the same cycle.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Partial Progressive Leak Test Campaign", mode=CampaignMode.PROGRESSIVE.value)
        session.add(campaign)
        session.flush()

        # Create 4 available agents
        agents = [
            Agent(campaign_id=campaign.id, name=f"Agent {i}", status=AgentState.AVAILABLE.value)
            for i in range(4)
        ]
        session.add_all(agents)

        # Create ONLY 1 eligible borrower
        borrower = Borrower(campaign_id=campaign.id, phone_number="+15551112222", name="Single Borrower")
        session.add(borrower)
        session.commit()
        cid = campaign.id
        agent_ids = [a.id for a in agents]

    # Execute progressive cycle
    with SessionMaker() as session:
        res = ProgressiveDialer.execute_cycle(
            session=session,
            campaign_id=cid,
            provider_name="provider_a",
        )
        session.commit()

        # Exactly 1 call was allocated
        assert len(res.allocations) == 1
        allocated_agent_id = res.allocations[0].agent.id

    # Verify agent states in database
    with SessionMaker() as session:
        for aid in agent_ids:
            ag = session.get(Agent, aid)
            if ag.id == allocated_agent_id:
                # Allocated agent is RESERVED for this call
                assert ag.status == AgentState.RESERVED.value
                assert ag.current_call_id is not None
            else:
                # ALL other 3 agents MUST BE AVAILABLE! NOT LEAKED IN RESERVED!
                assert ag.status == AgentState.AVAILABLE.value
                assert ag.reservation_token is None
                assert ag.current_call_id is None


def test_predictive_partial_borrower_exhaustion_cleans_up_agents(sync_test_engine):
    """
    When PredictiveDialer has claimed agents, but borrower pool runs dry,
    unallocated claimed agents are immediately released back to AVAILABLE.
    """
    SessionMaker = sessionmaker(bind=sync_test_engine, expire_on_commit=False)
    with SessionMaker() as session:
        campaign = Campaign(name="Partial Predictive Leak Test Campaign", mode=CampaignMode.PREDICTIVE.value)
        session.add(campaign)
        session.flush()

        # 3 available agents
        agents = [
            Agent(campaign_id=campaign.id, name=f"Pred Agent {i}", status=AgentState.AVAILABLE.value)
            for i in range(3)
        ]
        session.add_all(agents)
        session.commit()
        cid = campaign.id
        agent_ids = [a.id for a in agents]

    # Execute predictive cycle with 0 borrowers
    with SessionMaker() as session:
        res = PredictiveDialer.execute_cycle(
            session=session,
            campaign_id=cid,
            provider_name="provider_a",
        )
        session.commit()

        assert len(res.allocations) == 0

    # Verify ALL 3 agents are back in AVAILABLE
    with SessionMaker() as session:
        for aid in agent_ids:
            ag = session.get(Agent, aid)
            assert ag.status == AgentState.AVAILABLE.value
            assert ag.reservation_token is None
