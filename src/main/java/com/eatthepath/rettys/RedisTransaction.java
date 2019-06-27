package com.eatthepath.rettys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A Redis transaction is a group of commands to be executed atomically.
 *
 * @see <a href="https://redis.io/topics/transactions">Transactions</a>
 *
 * @see RedisClient#doInTransaction(Consumer)
 */
class RedisTransaction implements RedisCommandExecutor {

    private final RedisCommand<Void> multiCommand;
    private final List<RedisCommand> commands;
    private final RedisCommand<Object[]> execCommand;

    private static final Logger log = LoggerFactory.getLogger(RedisTransaction.class);

    RedisTransaction() {
        multiCommand = RedisCommandFactory.buildMultiCommand();
        commands = new ArrayList<>();
        execCommand = RedisCommandFactory.buildExecCommand();

        multiCommand.getFuture().whenComplete((v, cause) -> {
            if (cause != null) {
                log.error("Failed to start transaction", cause);
            }
        });

        execCommand.getFuture().whenComplete((responseArray, cause) -> {
            if (cause != null) {
                commands.forEach(command -> command.getFuture().completeExceptionally(cause));
            } else {
                if (commands.size() != responseArray.length) {
                    log.error("Command/response count mismatch: {} commands, {} responses", commands.size(), responseArray.length);
                }

                for (int i = 0; i < responseArray.length; i++) {
                    final RedisCommand command = commands.get(i);

                    //noinspection unchecked
                    command.getFuture().complete(command.getResponseConverter().apply(responseArray[i]));
                }
            }
        });
    }

    @Override
    public <T> CompletableFuture<T> executeCommand(final RedisCommand<T> command) {
        commands.add(command);
        return command.getFuture();
    }

    RedisCommand<Void> getMultiCommand() {
        return multiCommand;
    }

    List<RedisCommand> getCommands() {
        return commands;
    }

    RedisCommand<Object[]> getExecCommand() {
        return execCommand;
    }

    @Override
    public CompletableFuture<Void> multi() {
        throw new UnsupportedOperationException("Cannot issue MULTI commands inside of a transaction.");
    }

    // TODO Also throw UnsupportedOprtationException for other transactional commands, pub/sub commands, etc.
}
