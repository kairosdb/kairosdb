import {Component, EventEmitter, Input, OnChanges, OnInit, Output, SimpleChange} from '@angular/core';
import * as _ from 'lodash';
import {QueryStatus} from '../../model/query-status';

const numeral = require("numeral");

@Component({
    selector: 'kairos-query-status',
    templateUrl: './query-status.component.html'
})
export class QueryStatusComponent implements OnChanges, OnInit {

    @Input()
    public model: QueryStatus;

    public duration: string;
    public sampleSize: string;
    public dataPoints: string;

    public errorHeader: string;
    public errorMessage: string;

    public constructor() {

    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if (changes['model'] && this.model) {
            if (this.model.status === 'success') {
                this.duration = '';
                this.sampleSize = '';
                this.dataPoints = '';
                this.duration = this.formatDuration(this.model['duration']);
                this.computeStatistics();
            }
            else if (this.model.status === 'error') {
                this.errorHeader = '';
                this.errorMessage = '';
                this.formatErrorMessage();
            }
        }
    }

    ngOnInit() {
    }

    formatDuration(duration: number): string {
        if (duration) {
            return numeral(duration).format('0,0') + " ms";
        }
        else {
            return '';
        }
    }

    computeStatistics() {
        var dataPointCount = 0;
        var sampleSizeCount = 0;
        if (this.model.response && _.isArray(this.model.response)) {
            this.model.response.forEach(function (resultSet) {
                sampleSizeCount += resultSet['sample_size'];
                resultSet['results'].forEach(function (queryResult) {
                    dataPointCount += queryResult.values.length;
                });
            });
        }
        this.dataPoints = numeral(dataPointCount).format('0,0');
        this.sampleSize = numeral(sampleSizeCount).format('0,0');
    }

    formatErrorMessage() {
        if(!this.model.response) {
            this.errorMessage = 'Unkown error during query';
            return;
        }
        if(typeof this.model.response === 'string' || this.model.response instanceof String){
            this.errorMessage = <string> this.model.response;
        }
        else if(this.model.response['status']===0){
            this.errorMessage = 'No response from server';
        }
        else if(this.model.response['status']>=400){
            this.errorHeader = this.model.response['status'] + " " +this.model.response['statusText'];
            try {
                let jObject = JSON.parse(this.model.response['_body']);
                this.errorMessage = (jObject.errors) ? jObject.errors : this.model.response['_body'];
            } catch (e) {
                this.errorMessage = this.model.response['_body'];
            }
        }
    }

}
