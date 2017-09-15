import {Component, EventEmitter, Input, OnChanges, Output, SimpleChange} from '@angular/core';

const moment = require('moment-timezone');

@Component({
    selector: 'kairos-datetimepicker',
    templateUrl: 'datetime-picker.component.html',
    styleUrls: [ 'datetime-picker.component.css' ],
})
export class DateTimePickerComponent implements OnChanges{
    public date: String;
    public time: String;

    @Input()
    timestamp: number;
    @Output()
    timestampChange = new EventEmitter<number>();

    @Input()
    timezone: string;


    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        let dateTimeObject;
        if (this.timezone)
            dateTimeObject = moment.tz(this.timestamp, this.timezone);
        else
            dateTimeObject = moment(this.timestamp);
        this.date = dateTimeObject.format("YYYY-MM-DD");
        this.time = dateTimeObject.format("HH:mm:ss.SSS");

    }

    updateTimestamp(){
        let dateTimeObject;
        if (this.timezone)
            dateTimeObject = moment.tz(this.date + " " + this.time, this.timezone);
        else
            dateTimeObject = moment(this.date + " " + this.time);
        this.timestamp = dateTimeObject.valueOf();
        this.timestampChange.emit(this.timestamp);

    }



}
