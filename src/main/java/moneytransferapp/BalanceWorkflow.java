package moneytransferapp;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface BalanceWorkflow {

    @WorkflowMethod
    int getAccountBalance(String accountNumber);
}
