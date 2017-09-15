import {Component, ElementRef, EventEmitter, Input, OnInit, Output} from '@angular/core';

const moment = require('moment');

@Component({
    selector: 'kairos-datepicker',
    templateUrl: 'date-picker.component.html',
    styleUrls: [ 'date-picker.component.css' ],
})
export class DatePickerComponent implements OnInit{
    public dateObject: Date;

    @Input()
    dateModel: string;
    @Output()
    dateModelChange: EventEmitter<string> = new EventEmitter<string>();

    public showDatepicker: boolean = false;

    public constructor(private _eref: ElementRef) {
        document.addEventListener('click', this.offClickHandler.bind(this));
    }

    ngOnInit() {
        this.onTextEdit(this.dateModel);
    }

    showPopup() {
        //console.log('showPopup');
        this.showDatepicker = true;
    }

    hidePopup(event) {
        this.dateModel = moment(this.dateObject).format("YYYY-MM-DD");
        this.showDatepicker = false;
        this.dateModelChange.emit(this.dateModel);
    }

    offClickHandler(event: any) {
        //console.log('offClickHandler');
        if (!this._eref.nativeElement.contains(event.target) && this.showDatepicker) { // check click origin
            this.hidePopup(event);
        }
    }

    onTextEdit(fieldValue) {
        let tempDateObject = moment(fieldValue, "YYYY-MM-DD", true);
        if (tempDateObject.isValid()) {
            this.dateObject = tempDateObject.toDate();
        }
    }

}
