import {Component, ElementRef, EventEmitter, Input, OnChanges, Output, SimpleChange} from '@angular/core';
import {ActivatedRoute, Params} from '@angular/router';

@Component({
    selector: 'kairos-timepicker',
    templateUrl: './time-picker.component.html',
    styleUrls: ['./time-picker.component.css']
})
export class TimePickerComponent implements OnChanges {

    public hour: number;
    public minute: number;
    public second: number;
    public millisecond: number;

    @Input()
    timeModel: string;

    @Output()
    timeModelChange: EventEmitter<string> = new EventEmitter<string>();

    public showTimepicker: boolean = false;

    public constructor(private _eref: ElementRef) {
        document.addEventListener('click', this.offClickHandler.bind(this));
    }

    showPopup() {
        this.showTimepicker = true;
    }

    hidePopup(event) {
        this.showTimepicker = false;
        //this.timeModel = event;
        this.timeModelChange.emit(this.timeModel)
    }

    onSliderChange() {
        let secDefined = this.second && this.second != 0;
        let msDefined = this.millisecond && this.millisecond != 0;

        this.timeModel = ("0" + this.hour).slice(-2)
        + ":"
        + ("0" + this.minute).slice(-2)
        + ((secDefined || msDefined) ? ":" + ("0" + this.second).slice(-2) : "")
        + (msDefined ? "." + ("00" + this.millisecond).slice(-3) : "");
    }

    ngOnChanges(changes: { [propertyName: string]: SimpleChange }) {
        if(changes['timeModel']){
            if(this.timeModel.length===12 && this.timeModel.substr(this.timeModel.length-4)=='.000')
            this.timeModel=this.timeModel.substr(0,this.timeModel.length-4);
            if(this.timeModel.length===8 && this.timeModel.substr(this.timeModel.length-3)==':00')
            this.timeModel=this.timeModel.substr(0,this.timeModel.length-3);
            this.onTextEdit(this.timeModel);
        }
    }

    onTextEdit(fieldValue) {
        let parts = fieldValue.split(":");
        if (parts.length > 1) {
            this.hour = this.parseIntOrZero(parts[0]);
            this.minute = this.parseIntOrZero(parts[1]);
        }

        if (parts.length > 2) {
            let subparts = parts[2].split(".");
            if (subparts.length > 0) {
                this.second = this.parseIntOrZero(subparts[0]);
            }
            if (subparts.length > 1) {
                this.millisecond = this.parseIntOrZero(subparts[1]);
            }
            else {
                this.millisecond = 0;
            }
        }
        else {
            this.second = 0;
            this.millisecond = 0;
        }
    }

    onBlur(timeField: HTMLInputElement) {
        this.hour = Math.min(23, this.hour);
        this.minute = Math.min(59, this.minute);
        this.second = Math.min(59, this.second);
        this.millisecond = Math.min(999, this.millisecond);
        this.onSliderChange();
        timeField.value=this.timeModel;
    }

    offClickHandler(event: any) {
        if (!this._eref.nativeElement.contains(event.target) && this.showTimepicker) { // check click origin
            this.hidePopup(event);
        }

    }

    parseIntOrZero(str: string): number {
        let parsing = parseInt(str, 10);
        if (!isNaN(parsing)) {
            return parsing;
        } else {
            return 0;
        }
    }
}
