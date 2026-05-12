# Databricks AI Capabilities Meeting Prep

Prepared for leadership discussion on AI enablement, engineering maturity, and platform resilience.

---

## Executive framing

This discussion should be positioned as an operating model conversation, not just a product feature review. The central question is:

**Can Databricks help us scale AI usage in a way that is visible, governable, cost-aware, and resilient in production?**

The three themes below connect naturally:

- AI enablement explains where value and cost are coming from.
- Engineering maturity explains how AI moves from experiments into standardized delivery.
- Platform resilience explains whether the organization can run these workloads safely at scale.

---

## 1. AI Enablement, Tools, and Inference Growth

### What leadership should focus on

AI growth is usually not driven only by isolated experiments. It expands because more teams start using AI in day-to-day workflows and because production use cases often involve multiple model calls per transaction.

To make the discussion concrete, separate AI usage into workload types:

- `Developer productivity` - code assistants, notebook copilots, code explanation, documentation help
- `Data and analytics productivity` - natural-language-to-SQL, dashboard authoring, BI chat
- `Embedded product AI` - chatbots, copilots, summarization, recommendations, search
- `Batch AI` - enrichment, document processing, extraction, classification
- `Agentic workflows` - multi-step workflows with retrieval, tool use, planning, and automation

### What drives inference expansion

Leadership will likely ask why inference costs and usage rise quickly. Common reasons include:

- Wider employee adoption across engineering, analytics, and business teams
- Agentic patterns that generate several model calls for one user request
- RAG architectures adding embeddings, retrieval, reranking, and generation
- Batch workloads moving from pilot scale to enterprise scale
- Continuous evaluation and production monitoring invoking additional model calls
- Retry behavior, poor prompt design, and duplicated traffic increasing unnecessary token use

### Databricks capability positioning

Databricks now has a stronger AI platform story than "model hosting on notebooks." Relevant capability areas include:

- `Mosaic AI Model Serving` for serving models and agents
- `Mosaic AI Vector Search` for retrieval and RAG patterns
- `Unity AI Gateway` for rate limits, usage tracking, and guardrails
- `MLflow 3` for tracing, evaluation, production monitoring, and human review workflows
- `Inference tables` and `system tables` for usage, latency, and cost analysis

This means the leadership discussion should move beyond whether Databricks can run AI workloads. The real question is whether it can help the organization control and operationalize them.

### Inventory of AI tools and usage patterns

Recommend asking for a structured inventory rather than a qualitative list. A useful inventory should include:

- Business owner
- Technical owner
- Use case and criticality
- Model or provider being used
- Traffic pattern: interactive, batch, or agentic
- Data sensitivity level
- Environment: experiment, pre-production, or production
- Cost center or department
- Success metric
- Current maturity state

### Controls for experimentation vs. production AI

This is one of the most important maturity topics. The goal is not to slow experimentation; the goal is to avoid uncontrolled production growth.

Suggested control model:

#### Experimentation / sandbox

- Broad access for low-risk experimentation
- Budget caps and quotas
- Lower service expectations
- Non-sensitive or masked data
- Rapid iteration with clear expiration or review points

#### Pre-production

- Approved model patterns
- Evaluation datasets and benchmark tasks
- Trace capture and review workflows
- Defined owner and success criteria
- Security and compliance checks

#### Production

- Formal promotion and approval path
- Rate limits and concurrency controls
- Guardrails for harmful content and PII handling
- Monitoring, alerting, and rollback plan
- SLOs, ownership, and runbooks

### Leadership takeaway

**Inference growth is not inherently a problem. Unmanaged inference growth is.**

The organization needs visibility into which workloads are generating spend, which ones are creating business value, and which ones are ready for production-scale support.

---

## 2. Genie Code Agent and Engineering Processes

### How to describe Genie Code

Databricks `Genie Code` should be positioned as a Databricks-native AI assistant and agent for data-centric work. It appears strongest in areas such as:

- Notebooks
- SQL workflows
- ETL and data pipelines
- Machine learning workflows
- Dashboard and analytics authoring
- Work that benefits from Unity Catalog context, table metadata, and lineage

It should not be discussed only as a "code generation" tool. It is better framed as a way to standardize AI-assisted work inside the Databricks platform.

### The real problem to solve

Most organizations already have AI usage, but it is fragmented:

- Teams use different tools with no shared standards
- Prompts and instructions live in personal chats or notebooks
- Automation quality depends on individual habits
- There is no consistent distinction between experimentation and production artifacts
- AI-generated assets are not always reviewed or reproducible

The leadership question should therefore be:

**Can Genie Code help move us from fragmented personal automation to a standardized engineering workflow?**

### Comparison to Claude-code-style engineering practices

This comparison should be made at the workflow level rather than just model quality.

#### Claude-code-style practices usually emphasize

- Repository-centric engineering
- Strong local codebase context
- Fast edit, test, and refactor loops
- PR-based collaboration and review
- Broad applicability across backend, frontend, services, and infrastructure code

#### Genie Code appears strongest for

- Databricks-native workflows
- Data engineering and analytics work
- Notebook and SQL-centric development
- Pipelines, jobs, dashboards, and governed data assets
- Leveraging Unity Catalog metadata and lineage context

#### Balanced leadership view

- For data platform, analytics, SQL, notebooks, and ML workflows, Genie Code can be a strong fit.
- For broader software engineering across polyglot repositories and non-Databricks application stacks, repo-centric coding agents may still be stronger.
- In many organizations, these tools will coexist rather than fully replace one another.

### Moving from fragmented automation to standardized workflows

Recommend a target workflow like this:

1. Ideate in a sandbox or low-risk environment
2. Capture reusable prompts, instructions, or patterns
3. Create tests and evaluation datasets
4. Run quality gates and review checks
5. Promote through defined environments
6. Trace and monitor behavior in production
7. Improve based on cost, quality, and incident data

### Governance for AI-assisted development

This is where AI-assisted engineering becomes a discipline rather than an ad hoc productivity boost.

Recommended governance controls:

- Mandatory PR and peer review for production-impacting changes
- CI/CD gates for tests, linting, and security scans
- Evaluation suites for prompts, agents, and retrieval workflows
- Versioning of prompts, instructions, and model configurations
- Secrets management and prevention of prompt leakage
- Dependency scanning and package allowlists
- Traceability for model calls and human approvals where needed
- Least-privilege data access
- Clear ownership of every production AI endpoint, prompt, and workflow

### Warning signs of immaturity

Call out these risks if relevant:

- AI-generated code enters production without normal review controls
- Prompts change without versioning or approval
- No golden dataset exists for regression testing AI behavior
- Experimentation and production endpoints are mixed together
- No attribution exists for AI spend by team or use case
- Teams are using AI tools with inconsistent security practices

### Leadership takeaway

**AI-assisted development should increase throughput without lowering the bar for quality, security, reproducibility, or auditability.**

---

## 3. Platform Resilience and Recent Outages

### How to frame the outage discussion

Do not let this turn into a blame exercise or a vague reliability conversation. Keep the discussion structured around three questions:

- What failed?
- Why was the failure possible?
- Why did the organization not detect, contain, or recover faster?

### Suggested postmortem review structure

For each recent incident, ask for:

- Timeline of events
- Customer or business impact
- Detection time
- Mitigation time
- Recovery time
- Immediate technical cause
- Contributing process or architecture causes
- Preventive actions
- Named owner and due date for each follow-up action

### Likely root cause categories for AI platforms

For Databricks-based or similar AI workloads, typical incident patterns include:

- Endpoint scaling or cold-start issues
- Upstream model or provider dependency failures
- Quota exhaustion or rate-limit breaches
- Retry storms that amplify load
- Shared infrastructure between experiments and production
- Misconfigured routing, guardrails, or policies
- RAG failures caused by stale indexes or retrieval lag
- Schema drift or data contract issues
- Missing backpressure, queueing, or circuit breakers
- Weak observability across application, retrieval, model, and infrastructure layers

### Systemic gaps leadership should care about

The direct trigger matters, but recurring weaknesses matter more. Common systemic gaps are:

- No service tiering for critical vs non-critical AI workloads
- No SLOs or reliability targets for important endpoints
- Weak ownership and unclear operational accountability
- No fallback model or dependency strategy
- No capacity planning for inference growth
- Incomplete runbooks and incident response drills
- Limited end-to-end traceability from request to retrieval to response

### Monitoring, scaling, and dependency improvements

Databricks capabilities relevant to resilience include:

- Endpoint health metrics for latency, errors, CPU, and memory
- `Inference tables` for request and response analysis
- `MLflow tracing` for end-to-end GenAI observability
- Production monitoring with evaluators or judges
- `System tables` for historical usage and cost analysis
- `Unity AI Gateway` for rate controls and policy guardrails

Recommended resilience improvements:

- Define SLOs for critical AI services
- Isolate production traffic from experimentation traffic
- Add rate limits, concurrency protections, and quotas
- Introduce fallback models or fallback behavior where practical
- Load-test realistic traffic patterns before scale-up
- Map critical dependencies and known failure domains
- Create runbooks for high-probability failure scenarios
- Run tabletop exercises for outage response

### Leadership takeaway

**Outages usually do not come from one isolated defect. They reveal that usage growth has outpaced operational discipline.**

---

## Key outcomes to drive in the meeting

### 1. Visibility into AI cost and usage drivers

Push for:

- A complete AI workload inventory
- Clear attribution of usage and cost by team, use case, and environment
- A view of which workloads are driving inference expansion

### 2. Roadmap for mature AI-assisted engineering

Push for:

- A standard path from experimentation to production
- Clear guidance on where Genie Code should be the default pattern
- Agreed governance for code quality, security, traceability, and CI/CD

### 3. Resilience improvements to reduce outage risk

Push for:

- A common postmortem pattern
- Agreement on the top systemic gaps
- A short list of high-impact resilience improvements for the next 90 days

---

## Strong questions to ask in the meeting

- Do we have a full inventory of AI use cases, owners, and environments?
- Can we attribute AI cost and inference volume by workload and business owner?
- Which workloads are driving the fastest growth in model usage?
- What is our formal path from AI experimentation to production release?
- Where should Genie Code be the default workflow, and where should it not?
- What are the non-negotiable controls for AI-assisted development?
- What were the common patterns across recent outages?
- Which resilience improvements will materially reduce risk in the next quarter?

---

## Suggested 90-day roadmap

### First 30 days

- Build the AI workload inventory
- Assign business and technical owners
- Separate experimentation from production endpoints and usage
- Review recent incidents and identify the top systemic gaps

### Days 30-60

- Standardize the AI SDLC for code, prompts, agents, and evaluation
- Turn on usage, cost, and tracing visibility
- Define approved model patterns, guardrails, and rate limits
- Establish minimum governance for AI-assisted development

### Days 60-90

- Set SLOs for critical AI services
- Add fallback and dependency resilience patterns
- Create runbooks and incident response drills
- Measure improvement in cost visibility, engineering maturity, and outage risk

---

## Closing message for leadership

**The goal is not just more AI activity. The goal is governed, measurable, and resilient AI at scale.**

Databricks should be evaluated not only on feature breadth, but on whether it helps the organization industrialize AI responsibly across cost, engineering process, and operational resilience.
