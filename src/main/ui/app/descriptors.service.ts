import {Injectable} from '@angular/core';
import 'rxjs/add/operator/toPromise';
import {QueryService} from './query.service';
import {PsDescriptor} from './model/ps';

@Injectable()
export class DescriptorService {

    constructor(private queryService: QueryService) { }

    getDescriptorList(): Promise<PsDescriptor[]> {
        return this.queryService.getProcessingChain();
    }
}
