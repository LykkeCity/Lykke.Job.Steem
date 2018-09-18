import { Settings } from "../common";
import { AzureEntity, Int64, AzureRepository } from "./azure";

export class ParamsEntity extends AzureEntity {
    LastProcessedIrreversibleBlockTime: Date;

    @Int64()
    NextActionSequence: number;
}

export class ParamsRepository extends AzureRepository {

    private tableName: string = "SteemParams";
    private partitionKey = "Params";
    private rowKey = "";

    constructor(private settings: Settings) {
        super(settings.SteemJob.Azure.ConnectionString);
    }

    async get(): Promise<ParamsEntity> {
        return await this.select(ParamsEntity, this.tableName, this.partitionKey, this.rowKey);
    }

    async upsert(params: { nextActionSequence?: number, lastProcessedIrreversibleBlockTime?: Date }) {
        const entity = new ParamsEntity();
        entity.PartitionKey = this.partitionKey;
        entity.RowKey = this.rowKey;
        entity.NextActionSequence = params.nextActionSequence;
        entity.LastProcessedIrreversibleBlockTime = params.lastProcessedIrreversibleBlockTime;

        await this.insertOrMerge(this.tableName, entity);
    }
}