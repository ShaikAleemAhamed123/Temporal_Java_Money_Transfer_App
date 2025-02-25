// @@@SNIPSTART money-transfer-java-workflow-implementation
package moneytransferapp;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import io.temporal.common.RetryOptions;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class MoneyTransferWorkflowImpl implements MoneyTransferWorkflow {
    private static final Logger logger = LogManager.getLogger(MoneyTransferWorkflowImpl.class);
    private static final String WITHDRAW = "Withdraw";

    // RetryOptions specify how to automatically handle retries when Activities fail
    private final RetryOptions retryoptions = RetryOptions.newBuilder()
        .setInitialInterval(Duration.ofSeconds(1)) // Wait 1 second before first retry
        .setMaximumInterval(Duration.ofSeconds(20)) // Do not exceed 20 seconds between retries
        .setBackoffCoefficient(2) // Wait 1 second, then 2, then 4, etc
        .setMaximumAttempts(5000) // Fail after 5000 attempts
        .build();

    // ActivityOptions specify the limits on how long an Activity can execute before
    // being interrupted by the Orchestration service
    private final ActivityOptions defaultActivityOptions = ActivityOptions.newBuilder()
        .setRetryOptions(retryoptions) // Apply the RetryOptions defined above
        .setStartToCloseTimeout(Duration.ofSeconds(2)) // Max execution time for single Activity
        .setScheduleToCloseTimeout(Duration.ofSeconds(5000)) // Entire duration from scheduling to completion including queue time
        .build();

    private final Map<String, ActivityOptions> perActivityMethodOptions = new HashMap<String, ActivityOptions>() {{
        // A heartbeat time-out is a proof-of life indicator that an activity is still working.
        // The 5 second duration used here waits for up to 5 seconds to hear a heartbeat.
        // If one is not heard, the Activity fails.
        // The `withdraw` method is hard-coded to succeed, so this never happens.
        // Use heartbeats for long-lived event-driven applications.
        put(WITHDRAW, ActivityOptions.newBuilder().setHeartbeatTimeout(Duration.ofSeconds(5)).build());
    }};

    // ActivityStubs enable calls to methods as if the Activity object is local but actually perform an RPC invocation
    private final AccountActivity accountActivityStub = Workflow.newActivityStub(AccountActivity.class, defaultActivityOptions, perActivityMethodOptions);

    // The transfer method is the entry point to the Workflow
    // Activity method executions can be orchestrated here or from within other Activity methods
    @Override
    public String transfer(TransactionDetails transaction) {
        // Retrieve transaction information from the `transaction` instance
        String sourceAccountId = transaction.getSourceAccountId();
        String destinationAccountId = transaction.getDestinationAccountId();
        String transactionReferenceId = transaction.getTransactionReferenceId();
        int amountToTransfer = transaction.getAmountToTransfer();



        // Run the child workflow before moving on to the actual activity of the parent workflow

        logger.info("======================================================");
        logger.info("STARTING :: THE CHILD WORKFLOW");
        logger.info("======================================================");

        try{
            BalanceWorkflow childWorkflow = Workflow.newChildWorkflowStub(BalanceWorkflow.class);

            // Execute the child workflow asynchronously
            Promise<Integer> balancePromise = Async.function(childWorkflow::getAccountBalance, sourceAccountId);

            // Wait for the result
            int balance = balancePromise.get();

            if (balance < amountToTransfer) {
                logger.error("ERROR :: NOT ENOUGH BALANCE TO PERFORM FUND TRANSFER");
                return "TRANSFER FAILED DUE TO INSUFFICIENT FUNDS";
            }

            logger.info("========================================================");
            logger.info("SUCCESS :: THE CHILD WORKFLOW EXECUTED SUCCESSFULLY");
            logger.info("========================================================");
        }
        catch(Exception e){
            logger.error("WHILE PROCESSING THE CHILD WORKFLOW {}",e.getMessage());
        }


        // Stage 1: Withdraw funds from source
        try {
            // Launch `withdrawal` Activity
            accountActivityStub.withdraw(sourceAccountId, transactionReferenceId, amountToTransfer);
        } catch (Exception e) {
            // If the withdrawal fails, for any exception, it's caught here
            logger.error("{} Withdrawal of {} from account {} failed.", transactionReferenceId, amountToTransfer, sourceAccountId);

            // Transaction ends here
            return "Withdrawal failed";
        }

        // Stage 2: Deposit funds to destination
        try {
            // Perform `deposit` Activity
            accountActivityStub.deposit(destinationAccountId, transactionReferenceId, amountToTransfer);

            // The `deposit` was successful
            logger.info("{} Transaction succeeded.", transactionReferenceId);

            //  Transaction ends here
            return "Transaction COMPLETE";
        } catch (Exception e) {
            // If the deposit fails, for any exception, it's caught here
            logger.warn("{} Deposit of {} to account {} failed.", transactionReferenceId, amountToTransfer, destinationAccountId);
        }

        // Continue by compensating with a refund

        try {
            // Perform `refund` Activity
            logger.info("{} Refunding {} to account {}.", transactionReferenceId, amountToTransfer, sourceAccountId);

            accountActivityStub.refund(sourceAccountId, transactionReferenceId, amountToTransfer);

            // Recovery successful. Transaction ends here
            logger.info("{} Refund to originating account was successful.", transactionReferenceId);
            logger.info("{} Transaction is complete. No transfer made.", transactionReferenceId);
            return "REFUND COMPLETE";
        } catch (Exception e) {
            // A recovery mechanism can fail too. Handle any exception here
            logger.error("{} Deposit of {} to account {} failed. Did not compensate withdrawal.",transactionReferenceId, amountToTransfer, destinationAccountId);
            logger.error("{} Workflow failed.", transactionReferenceId);

            // Rethrowing the exception causes a Workflow Task failure
            throw(e);
        }
    }
}
// @@@SNIPEND
