//! Deterministically compiles an ACTIVE thesis contract and its resolved live
//! context into the state and questions consumed by TypeSafe Jev.
//!
//! This module intentionally has no provider/client dependencies and cannot
//! make model calls.

use crate::contracts::{ContractState, HypothesisContract};
use crate::domain::ResolvedJevState;
use anyhow::{bail, Result};

#[derive(Debug, Clone)]
pub struct CompiledJevInput {
    pub state: ResolvedJevState,
    pub jev1_question: String,
    pub jev2_question: String,
}

pub fn compile(
    contract: &HypothesisContract,
    mut state: ResolvedJevState,
) -> Result<CompiledJevInput> {
    if contract.state != ContractState::Active {
        bail!("JevCompiler accepts only an ACTIVE HypothesisContract");
    }
    if contract.proposal.jev1_objective.trim().is_empty()
        || contract.proposal.jev2_objective.trim().is_empty()
    {
        bail!("ACTIVE contract is missing Jev objectives");
    }
    let snapshot = state
        .live_context_snapshot
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("JevCompiler requires a resolved live-context snapshot"))?;
    if snapshot.freshness_state != "fresh" || snapshot.quality_state.trim().is_empty() {
        bail!("JevCompiler rejected a stale or incomplete live-context snapshot");
    }
    let supplied_fields: std::collections::HashSet<&str> = snapshot
        .fields
        .iter()
        .map(|field| field.field_id.as_str())
        .collect();
    for requirement in contract
        .proposal
        .context_requirements
        .iter()
        .filter(|requirement| requirement.required)
    {
        if requirement.value_type == crate::contracts::ContextValueType::Indicator
            && !supplied_fields.contains(requirement.id.as_str())
        {
            bail!(
                "JevCompiler is missing required live field {}",
                requirement.id
            );
        }
    }
    state.thesis = contract.proposal.thesis.clone();
    state.instruments = contract.proposal.instruments.clone();
    state.timeframe = contract.proposal.timeframe.clone();
    Ok(CompiledJevInput {
        state,
        jev1_question: contract.proposal.jev1_objective.clone(),
        jev2_question: contract.proposal.jev2_objective.clone(),
    })
}
