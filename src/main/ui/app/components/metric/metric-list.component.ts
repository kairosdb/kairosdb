import {
    Component,
    EventEmitter,
    Input,
    OnChanges,
    OnInit,
    Output,
    QueryList,
    SimpleChange,
    ViewChildren
} from '@angular/core';
import * as _ from 'lodash';

@Component({
    selector: 'kairos-metric-list',
    templateUrl: './metric-list.component.html',
    styleUrls: [ './metric-list.component.css' ]
})
export class MetricListComponent implements OnChanges, OnInit {
    @Input()
    public parsedMetricList: {}[];

    public workingMetricList: {}[];

    public generatedMetricList: {}[];

    @Output()
    public metricListChange = new EventEmitter<{}[]>();

    public selectedMetricIndex: number;

    ngOnInit() {
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if(changes['parsedMetricList']){
            this.selectedMetricIndex = this.parsedMetricList ? this.parsedMetricList.length-1 : undefined;
            this.workingMetricList = _.map(this.parsedMetricList,_.identity) || [];
            this.generatedMetricList = _.map(this.parsedMetricList,_.identity) || [];
        }
    }

    onPanelToggle(idx:number){
        this.selectedMetricIndex=(this.selectedMetricIndex===idx)?undefined:idx
    }

    onMetricEdit(idx, newMetricObject){
        this.generatedMetricList[idx]=newMetricObject;
        this.metricListChange.emit(this.generatedMetricList);
    }

    addNew() {
        this.workingMetricList.push({});
        this.generatedMetricList.push({});
        this.metricListChange.emit(this.generatedMetricList);
        this.selectedMetricIndex=this.workingMetricList.length-1;
    }

    deleteMetric(idx: number){
        if(idx===this.selectedMetricIndex && idx<this.generatedMetricList.length){
            this.selectedMetricIndex=undefined;
        }
        else if(this.selectedMetricIndex && idx<this.selectedMetricIndex){
            this.selectedMetricIndex--;
        }
        _.pullAt(this.generatedMetricList, idx);
        _.pullAt(this.workingMetricList, idx);
        this.metricListChange.emit(this.generatedMetricList);
    }

    duplicateMetric(idx: number){
        this.workingMetricList.push(_.cloneDeep(this.generatedMetricList[idx]));
        this.generatedMetricList.push(_.cloneDeep(this.generatedMetricList[idx]));
        this.metricListChange.emit(this.generatedMetricList);
    }

}
