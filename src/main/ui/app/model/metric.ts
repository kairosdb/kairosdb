export class Metric {
    name: string;
    tags: {};

    constructor(name?: string) {
        this.name = name;
        this.tags = {};
    }
}
