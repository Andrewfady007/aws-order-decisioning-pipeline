# Event-Driven Order Decisioning Pipeline

An event-driven pipeline on AWS that automatically approves, flags for review,
or declines customer orders based on multiple fraud and risk signals.

## Architecture

```
Orders (CSV) --> DynamoDB Orders Table --> Lambda 1 (Decision Engine)
                                                  |
                                                  v
                                          DynamoDB Decisions Table
                                          (audit trail of every decision)
                                                  |
                                                  v
                                          Lambda 2 (Action Handler)
                                                  |
                                                  v
                                  fulfil / queue for review / retry later
```

- **Lambda 1 (Decision Engine)**: reads new orders and decides
  `AUTO_APPROVE`, `MANUAL_REVIEW`, or `DECLINE` using at least four signals:
  fraud score, basket value, payment method, and customer tenure.
- **Lambda 2 (Action Handler)**: reads each decision and takes the
  corresponding downstream action (fulfil / queue for review / retry).
- Every decision is written back to DynamoDB as a full audit record.

## Project structure

```
data/             Generated CSV datasets (100,000 fake orders with fraud anomalies)
lambdas/           Lambda function source code
  decision_lambda/  Lambda 1 - order decisioning
  action_lambda/    Lambda 2 - downstream actions
infrastructure/    AWS CDK (Python) app defining all infrastructure
scripts/           Seed and test scripts (data generation, DynamoDB loaders, smoke tests)
```

## Tooling

This project was built with:
- Python 3.12
- AWS CDK 2.x (Python)
- AWS CLI v2
- DynamoDB, Lambda (all within AWS Free Tier limits)

## Status

Project scaffolding complete. Next steps (to be filled in as the build progresses):

- [ ] Generate 100,000 fake orders with fraud anomalies
- [ ] Define DynamoDB tables (Orders, Decisions) in CDK
- [ ] Implement Lambda 1: decision engine
- [ ] Implement Lambda 2: action handler
- [ ] Wire Lambdas together (DynamoDB Streams / EventBridge)
- [ ] Seed data into DynamoDB
- [ ] Test end-to-end pipeline
- [ ] Document signals, decision logic, and architecture diagram
