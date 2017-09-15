import {NgModule} from '@angular/core';
import {CommonModule} from '@angular/common';
import {FormsModule} from '@angular/forms';
import {AlertModule, ButtonsModule, DatepickerModule, TypeaheadModule} from 'ng2-bootstrap/ng2-bootstrap';

import {SharedModule} from './shared/shared.module';
import {DatePickerComponent} from './components/time/date-picker.component';
import {DateTimePickerComponent} from './components/time/datetime-picker.component';
import {TimePickerComponent} from './components/time/time-picker.component';
import {RelativePickerComponent} from './components/time/relative-picker.component';
import {TimeRangeComponent} from './components/time/time-range.component';

@NgModule({
    imports:      [ CommonModule, FormsModule, DatepickerModule.forRoot(), AlertModule.forRoot(), ButtonsModule.forRoot(), SharedModule ],
    declarations: [ TimePickerComponent, DatePickerComponent, DateTimePickerComponent, RelativePickerComponent, TimeRangeComponent],
    exports:      [ TimeRangeComponent ]
})
export class TimeRangeModule { }
