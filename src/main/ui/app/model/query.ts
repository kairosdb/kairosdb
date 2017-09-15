import {Metric} from './metric';

export class Query {
    time_zone: string;
    start_relative: {};
    start_absolute: number;
    end_relative: {};
    end_absolute: number;
    metrics: Metric[];
}
