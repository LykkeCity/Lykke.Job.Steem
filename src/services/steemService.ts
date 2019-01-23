import { Settings, ADDRESS_SEPARATOR, isoUTC, isSteemAddress } from "../common";
import { LogService, LogLevel } from "./logService";
import { AssetRepository } from "../domain/assets";
import { OperationRepository, ErrorCode } from "../domain/operations";
import { ParamsRepository } from "../domain/params";
import { BalanceRepository } from "../domain/balances";
import { HistoryRepository } from "../domain/history";

const steem = require("steem");

export class SteemService {

    private paramsRepository: ParamsRepository;
    private balanceRepository: BalanceRepository;
    private assetRepository: AssetRepository;
    private operationRepository: OperationRepository;
    private historyRepository: HistoryRepository;
    private log: (level: LogLevel, message: string, context?: any) => Promise<void>;

    constructor(private settings: Settings, private logService: LogService) {
        steem.api.setOptions({
            url: settings.SteemJob.Steem.Url,
            useAppbaseApi: true
        });
        this.paramsRepository = new ParamsRepository(settings);
        this.balanceRepository = new BalanceRepository(settings);
        this.assetRepository = new AssetRepository(settings);
        this.operationRepository = new OperationRepository(settings);
        this.historyRepository = new HistoryRepository(settings);
        this.log = (l, m, c) => this.logService.write(l, SteemService.name, this.handleActions.name, m, JSON.stringify(c));
    }

    async handleActions(): Promise<number> {
        const params = await this.paramsRepository.get();
        const globalProperties = await steem.api.getDynamicGlobalPropertiesAsync();
        
        let nextActionSequence = (params && params.NextActionSequence) || 0;
        
        while (true) {
            const accHistory = await steem.api.getAccountHistoryAsync(this.settings.SteemJob.HotWalletAccount, nextActionSequence, 0);
            const action = !!accHistory && !!accHistory.length && accHistory[0][0] == nextActionSequence && accHistory[0][1];

            if (!!action && action.block <= globalProperties.last_irreversible_block_num) {
               
                const transfer = action.op[0] == "transfer" && action.op[1];
                const block = action.block * 10;
                const blockTime = isoUTC(action.timestamp);
                const txId = action.trx_id;
                // in testnet action number within transaction (op_in_trx)
                // is 0 for all transaction actions due to some errors,
                // so use global action number as action ID
                const actionId = nextActionSequence.toString();

                await this.log(LogLevel.info, `${action.op[0]} action ${!!transfer ? "detected" : "skipped"}`, {
                    Account: this.settings.SteemJob.HotWalletAccount,
                    Seq: nextActionSequence
                });
                
                if (!!transfer) {
                    const operationId = await this.operationRepository.getOperationIdByTxId(txId);
                    if (!!operationId) {

                        // this is our operation, so use our data 
                        // to record balance changes and history

                        const operationActions = await this.operationRepository.getActions(operationId);
                        const operation = await this.operationRepository.get(operationId);

                        if (!operation.isCompleted()) {
                            for (const action of operationActions) {
                                // record balance changes
                                const balanceChanges = [
                                    { address: action.FromAddress, affix: -action.Amount, affixInBaseUnit: -action.AmountInBaseUnit },
                                    { address: action.ToAddress, affix: action.Amount, affixInBaseUnit: action.AmountInBaseUnit }
                                ];
                                for (const bc of balanceChanges) {
                                    await this.balanceRepository.upsert(bc.address, operation.AssetId, operationId, bc.affix, bc.affixInBaseUnit, block);
                                    await this.log(LogLevel.info, "Balance change recorded", {
                                        ...bc, assetId: operation.AssetId, txId
                                    });
                                }

                                // upsert history of operation action
                                await this.historyRepository.upsert(action.FromAddress, action.ToAddress, operation.AssetId, action.Amount, action.AmountInBaseUnit,
                                    block, blockTime, txId, action.RowKey, operationId);
                            }

                            // set operation state to completed
                            await this.operationRepository.update(operationId, { completionTime: new Date(), blockTime, block });
                        }
                    } else {

                        // this is external transaction, so use blockchain 
                        // data to record balance changes and history

                        // get amount and asset
                        const parts = transfer.amount.split(" ", 2);
                        const value = parseFloat(parts[0]);
                        const asset = await this.assetRepository.get(parts[1]);

                        if (!!asset) {
                            const assetId = asset.AssetId;
                            const valueInBaseUnit = asset.toBaseUnit(value);
                            const to = !!transfer.memo
                                ? transfer.to + ADDRESS_SEPARATOR + transfer.memo
                                : transfer.to;

                            if (isSteemAddress(to)) {
                                // record history
                                await this.historyRepository.upsert(transfer.from, to, assetId, value, valueInBaseUnit, block, blockTime, txId, actionId, operationId);
                                await this.log(LogLevel.info, "Transfer recorded", transfer);

                                // record balance changes
                                const balanceChanges = [
                                    { address: transfer.from, affix: -value, affixInBaseUnit: -valueInBaseUnit },
                                    { address: to, affix: value, affixInBaseUnit: valueInBaseUnit }
                                ];
                                for (const bc of balanceChanges) {
                                    await this.balanceRepository.upsert(bc.address, assetId, txId, bc.affix, bc.affixInBaseUnit, block);
                                    await this.log(LogLevel.info, "Balance change recorded", {
                                        ...bc, assetId, txId
                                    });
                                }
                            }
                            else {
                                await this.log(LogLevel.warning, "Invalid destination address", to);
                            }
                        } else {
                            await this.log(LogLevel.warning, "Not tracked token", parts[1]);
                        }
                    }
                }

                // increment counter to fetch next action
                nextActionSequence++;

                // update state
                await this.paramsRepository.upsert({
                    nextActionSequence: nextActionSequence
                });
            } else {
                break;
            }
        }

        return globalProperties.last_irreversible_block_num;
    }

    async handleExpired(lastActionIrreversibleBlockNumber: number) {

        // some actions may come after handleActions() and before handleExpired() calling,
        // such operations will be wrongly marked as failed if we get last irreversible block from getInfo() here,
        // that's why we must use last irreversible block from getActions()

        const params = await this.paramsRepository.get();
        const lastProcessedIrreversibleBlockTime = (params && params.LastProcessedIrreversibleBlockTime) || new Date(0);
        const lastActionIrreversibleBlock = await steem.api.getBlockAsync(lastActionIrreversibleBlockNumber);
        const lastActionIrreversibleBlockTime = isoUTC(lastActionIrreversibleBlock.timestamp);

        // mark expired operations as failed, if any

        const presumablyExpired = await this.operationRepository.geOperationIdByExpiryTime(lastProcessedIrreversibleBlockTime, lastActionIrreversibleBlockTime);

        for (let i = 0; i < presumablyExpired.length; i++) {
            const operation = await this.operationRepository.get(presumablyExpired[i])
            if (!!operation && !operation.isCompleted() && !operation.isFailed()) {
                await this.log(LogLevel.warning, "Transaction expired", operation.OperationId);
                await this.operationRepository.update(operation.OperationId, {
                    errorCode: ErrorCode.buildingShouldBeRepeated,
                    error: "Transaction expired",
                    failTime: new Date()
                });
            }
        }

        // update state

        await this.paramsRepository.upsert({
            lastProcessedIrreversibleBlockTime: lastActionIrreversibleBlockTime
        });
    }
}