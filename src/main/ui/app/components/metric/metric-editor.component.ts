import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChange} from '@angular/core';
import {TypeaheadMatch} from 'ng2-bootstrap/ng2-bootstrap';
import {Subject} from 'rxjs/Subject';
import * as _ from 'lodash';
import {QueryService} from '../../query.service';
import {DescriptorService} from '../../descriptors.service';
import {Metric} from '../../model/metric';
import {PsDescriptor} from '../../model/ps';

@Component({
    selector: 'kairos-metric-editor',
    templateUrl: './metric-editor.component.html',
    styleUrls: [ './metric-editor.component.css' ]
})
export class MetricEditorComponent implements OnChanges, OnInit {

    @Input()
    public parsedMetricObject: Metric;

    public generatedMetricObject: Metric;

    @Output()
    public metricObjectChange = new EventEmitter<Metric>();

    public metricNames: string[];

    private metricNameSubject: Subject<string>;

    public tagValuesForNames: {};

    public refreshingMetricNames: boolean;

    public descriptorList: PsDescriptor[];

    public duplicatedTagNames: string[];

    public constructor(private queryService: QueryService, private descriptorsService: DescriptorService) {
        // initialize empty arrays for typeahead component
        this.metricNames = [];
        this.tagValuesForNames = {};
        this.generatedMetricObject = new Metric();
        this.descriptorList = [];
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if(changes['parsedMetricObject']){
            this.generatedMetricObject = _.cloneDeep(this.parsedMetricObject);
        }
    }

    ngOnInit() {
        this.metricNameSubject = new Subject<string>();
        this.metricNameSubject.debounceTime(400).filter(value => value !== undefined && value !== null && value !== '').subscribe(
            metricName => this.queryService.getTagNameValues(metricName).then(
                resp => { this.tagValuesForNames = resp || {}; }
            )
        );
        this.descriptorsService.getDescriptorList().then(descList => {
            this.descriptorList = descList || [];
        });
        this.refreshMetricNames();
    }

    refreshMetricNames() {
        this.refreshingMetricNames = true;
        this.queryService.getMetricNames().then(resp => { this.refreshingMetricNames = false; this.metricNames = resp; });
        this.metricNameSubject.next(this.generatedMetricObject.name);
    }

    onMetricNameUpdate() {
        this.metricNameSubject.next(this.generatedMetricObject.name);
    }

}
