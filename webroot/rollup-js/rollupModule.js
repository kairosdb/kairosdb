var module = angular.module('rollupApp',
        ['mgcrea.ngStrap',
            'mgcrea.ngStrap.tooltip',
            'ui.bootstrap.modal',
            'template/modal/backdrop.html',
            'template/modal/window.html']);

module.directive('editable', function ($compile)
{
    var template = '' +
            '<span>' +
            '<input class="small-input" type="text" ' +
            '	ng-model="model"  ' +
            '	ng-blur="$parent.onBlur($parent.task)" ' +
            '	ng-show="edit" ' +
            '	my-blur="edit">' +
            '/>' +

            '<a href="" ' +
            '	ng-show="!edit && !$parent.task.complex" ' +
            '	ng-click="onClick()" ' +
            '	ng-class="model.indexOf(\'<\') == 0 ? \'gray\' : \'black\'">{{model}}</a>' +

            '	<span ng-show="$parent.task.complex">{{model}}</span> ' +
            '</span>';

    var linker = function (scope, element, attributes)
    {

        scope.onClick = function ()
        {
            if (scope.$parent.task.complex) {
                // Ignore complex tasks
                return;
            }

            scope.edit = false;

            element.bind('click', function ()
            {
                inputField = element.find('input').get(0);
                inputField.style.width = element[0].parentElement.offsetWidth - 15 + "px";
                if (inputField.value.indexOf("<") === 0) {
                    // Remove text from text field since its just placeholder text
                    inputField.value = '';
                }
                scope.$apply(scope.edit = true);
                inputField.focus();
            });
        };

        element.html(template).show();

        $compile(element.contents())(scope);
    };

    return {
        restrict: "E",
        link: linker,
        scope: {
            model: '='
        }
    };
});

module.directive('autocompleteeditable', function ()
{
    return {
        restrict: 'E',
        replace: false,
        template: '<span>' +
        '<input ' +
        'class="small-input"' +
        'type="text"' +
        'ng-show="edit"' +
        'my-blur="edit"' +
        'ng-blur="suggestSaveAs(task)"' +
        'ng-model="task.metric_name"' +
        'myblur="edit" spellcheck="false"' +
        'bs-typeahead bs-options="metric for metric in suggestMetrics(task.metric_name)"' +
        'placeholder="<metric name>"' +
        'min-length="0"' +
        'limit="METRIC_NAME_LIST_MAX_LENGTH"' +
        'ng-change="targetBlur()"' +
        'ng-blur="suggestSaveAs()"' +
        'data-provide="typeahead"' +
        'style="width:100%;"' +
        'ng-focus autofocus>' +
        '</>' +

        '<a href="" ' +
        '	ng-show="!edit && !task.complex" ' +
        '	ng-class="task.metric_name.indexOf(\'<\') == 0 ? \'gray\' : \'black\'"' +
        '>{{task.metric_name}}</a>' +

        '<span ng-show="task.complex">{{task.metric_name}}</span> ' +
        '</span>',
        link: function (scope, element, attrs)
        {
            if (scope.task.complex) {
                // Ignore complex tasks
                return;
            }

            scope.edit = false;
            element.bind('click', function ()
            {

                inputField = element.find('input').get(0);
                inputField = element.find('input').get(0);
                inputField.style.width = element[0].parentElement.offsetWidth - 15 + "px";
                if (inputField.value.indexOf("<") === 0) {
                    // Remove text from text field since its just placeholder text
                    inputField.value = '';
                }
                scope.$apply(scope.edit = true);
                inputField.focus();
            });
        }
    };
});

//blur directive
module.directive('myBlur', function ()
{
    return {
        restrict: 'A',
        link: function (scope, element, attr)
        {
            element.bind('blur', function ()
            {
                scope.edit = false;
                scope.$apply();
            });
        }
    };
});

module.directive('customPopover', function ()
{
    return {
        restrict: 'A',
        template: '<a href="#"><span class="glyphicon glyphicon-question-sign" aria-hidden="true"></span></a>',
        link: function (scope, el, attrs)
        {
            $(el).popover({
                trigger: 'focus',
                html: true,
                content: attrs.popoverHtml,
                placement: attrs.popoverPlacement
            });
        }
    };
});

module.directive('focusOnShow', function ($timeout)
{
    return {
        restrict: 'A',
        link: function ($scope, $element, $attr)
        {
            if ($attr.ngShow) {
                $scope.$watch($attr.ngShow, function (newValue)
                {
                    if (newValue) {
                        $timeout(function ()
                        {
                            $element.focus();
                        }, 0);
                    }
                })
            }
            if ($attr.ngHide) {
                $scope.$watch($attr.ngHide, function (newValue)
                {
                    if (!newValue) {
                        $timeout(function ()
                        {
                            $element.focus();
                        }, 0);
                    }
                })
            }

        }
    };
});

// // This is needed by dynamically created elements. The regular tooltip mechanism
// // does not work for dynamic elements
// module.directive('bsTooltip', function ()
// {
//     return {
//         restrict: 'A',
//         link: function (scope, element, attrs)
//         {
//             $(element).hover(function ()
//             {
//                 // on mouseenter
//                 $(element).tooltip('show');
//             }, function ()
//             {
//                 // on mouseleave
//                 $(element).tooltip('hide');
//             });
//         }
//     };
// });

module.directive('autocompleteWithButtons', function ($compile)
{
    var template = '' +
            '<table width="100%">' +
            '		<tr>' +
            '			<td>' +
            '			<span ng-show="!model.edit" style="margin: 0 10px 0 0">{{model.group_by_values}}</span>' +
            '			<input type="text" style="width:85px;"' +
            '               class="small-input"' +
            '				focus-on-show ' +
            '				spellcheck="false"' +
            '				bs-typeahead ' +
            '				bs-options="key for key in list(model)"' +
            '				data-min-length=0 ' +
            '				data-items=100' +
            '				ng-model="model.group_by_value_suggest"' +
            '				placeholder="key"' +
            '				ng-show="model.edit"' +
            '			/>' +
            '			</td>' +
            '			<td width="10%">' +
            '				<a href="#" class="btn-sm btn-circle text-right" style="margin-left:1px; margin-right:5px;"' +
            '					ng-click="model.edit = true"' +
            '					ng-show="!model.edit && !model.complex">' +
            '					<span class="glyphicon glyphicon-plus shift-1-up"></span>' +
            '				</a>' +
            '				<a href="#" class="btn-sm btn-circle" style="margin-right:10px;"' +
            '					ng-click="setGroupValues(model);onBlur(model)"' +
            '					ng-show="model.edit">' +
            '				<span class="glyphicon glyphicon-ok text-success"></span></a>' +
            '			</td>' +
            '			<td width="10%">' +
            '				<a href="#" class="btn-sm btn-circle" style="margin-left:1px; margin-right:1px;"' +
            '					ng-click="removeGroupValues(model);onBlur(model)"' +
            '					ng-show="model.group_by_values.length > 0 && !model.edit">' +
            '				<span class="glyphicon glyphicon-remove text-danger"></span></a>' +
            '					<a href="#" class="btn-sm btn-circle" style="margin-left:1px; margin-right:1px;"' +
            '						ng-click="cancelGroupValues(model)"' +
            '						ng-show="model.edit && !model.complex">' +
            '				<span class="glyphicon glyphicon-remove-sign text-danger"></span></a>' +
            '			</td>' +
            '		</tr>' +
            '</table>';

    var linker = function (scope, element, attributes)
    {
        element.html(template).show();

        $compile(element.contents())(scope);

        scope.cancelGroupValues = function (model)
        {
            model.edit = false;
            model.group_by_value_suggest = "";
        };

        scope.removeGroupValues = function (model)
        {
            if (model.group_by_values) {
                model.group_by_values = "";
            }
        };

        scope.setGroupValues = function (model)
        {
            model.edit = false;
            if (!model.group_by_values) {
                model.group_by_values = model.group_by_value_suggest;
            }
            else {
                model.group_by_values += ", " + model.group_by_value_suggest;
            }
            model.group_by_value_suggest = "";
        };
    };

    return {
        restrict: "E",
        link: linker,
        scope: {
            model: '=',
            list: '&',
            onBlur: '&'
        }
    };
});


module.directive('autocompleteTags', function ($compile)
{
    var template = '' +
            '<table width="100%">' +
            '		<tr>' +
            '			<td>' +
            '			<table ng-show="!model.tagEdit"> ' +
            '				<tr ng-repeat="(tag, value) in model.tags"> ' +
            '				<td> ' +
            '					{{tag}}=<span ng-repeat="val in value">{{val}}{{$last ? "" : ", "}}</span>' +
            '				</td> ' +
            '				</tr> ' +
            '			</table> ' +
            '			<input type="text" style="width:85px;' +
            '               class="small-input"' +
            '				focus-on-show ' +
            '				spellcheck="false"' +
            '				bs-typeahead ' +
            '				bs-options="key for key in keyList()"' +
            '				data-min-length=0 ' +
            '				data-items=100' +
            '				ng-model="model.currentTagKey"' +
            '				placeholder="key"' +
            '				ng-show="model.tagEdit"' +
            '			/>' +

            '			<input type="text" style="width:85px; padding:0; line-height:16px" ' +
            '				focus-on-show ' +
            '				class="input-small tight-form-input" ' +
            '				spellcheck="false" ' +
            '				bs-typeahead ' +
            '				bs-options="key for key in valueList()" ' +
            '				data-min-length=0 ' +
            '				data-items=100 ' +
            '				ng-model="model.currentTagValue" ' +
            '				placeholder="value" ' +
            '				ng-show="model.tagEdit"' +
            '			/>' +
            '			</td>' +
            '			<td width="10%" valign="top">' +
            '				<a href="#" class="btn-sm btn-circle text-right" style="margin-left:1px; margin-right:5px;"' +
            '					ng-click="model.tagEdit = true"' +
            '					ng-show="!model.tagEdit && !model.complex">' +
            '					<span class="glyphicon glyphicon-plus shift-1-up"></span>' +
            '				</a>' +
            '				<a href="#" class="btn-sm btn-circle" style="margin-right:10px;"' +
            '					ng-click="setTag(model);onBlur(model)"' +
            '					ng-show="model.tagEdit">' +
            '				<span class="glyphicon glyphicon-ok text-success"></span></a>' +
            '			</td>' +
            '			<td width="10%" valign="top">' +
            '				<a href="#" class="btn-sm btn-circle" style="margin-left:1px; margin-right:1px;"' +
            '					ng-click="removeTag(model);onBlur(model)"' +
            '					ng-show="model.tags && !model.tagEdit && !model.complex">' +
            '				<span class="glyphicon glyphicon-remove text-danger"></span></a>' +
            '					<a href="#" class="btn-sm btn-circle" style="margin-left:1px; margin-right:1px;"' +
            '						ng-click="cancelTagValues(model)"' +
            '						ng-show="model.tagEdit">' +
            '				<span class="glyphicon glyphicon-remove-sign text-danger"></span></a>' +
            '			</td>' +
            '		</tr>' +
            '</table>';

    var linker = function (scope, element, attributes)
    {
        element.html(template).show();

        $compile(element.contents())(scope);

        scope.cancelTagValues = function (model)
        {
            model.tagEdit = false;

            delete model.currentTagKey;
            delete model.currentTagValue
        };

        scope.removeTag = function (model)
        {
            delete model.tags;
        };

        scope.setTag = function (model)
        {
            model.tagEdit = false;
            if (!model.tags) {
                model.tags = {};
            }

            var existingTag = [];
            if (model.tags[model.currentTagKey]) {
                existingTag = model.tags[model.currentTagKey]
            }
            existingTag.push(model.currentTagValue);
            model.tags[model.currentTagKey] = existingTag;

            delete model.currentTagKey;
            delete model.currentTagValue;
        };
    };

    return {
        restrict: "E",
        link: linker,
        scope: {
            model: '=',
            keyList: '&',
            valueList: '&',
            onBlur: '&'
        }
    };
});

module.directive('aggregator', function ($compile)
{
    return {
        link: function (scope, element, attributes)
        {
            var oldAgg = angular.copy(scope.agg);

            var html = '';
            html += '<table width="250px">';
            html += '<tr>';
            html += '	<td><aggregatornamedropdown agg="scope.agg.name" descriptors="scope.descriptors"></aggregatornamedropdown>';
            html += '       <table style="padding: 0">';
            html += '           <tr>';
            html += '               <td style="padding-left: 20px">';
            html += '                   <aggregatorproperties agg="scope.agg" descriptors="scope.descriptors">';
            html += '               </td>';
            html += '           </tr>';
            html += '       </table>';
            html += '   </td>';
            html += '   <td width="10%" valign="top">';
            html += '		<a href="#" class="btn-sm btn-circle" style="margin-right:10px;"';
            html += '			ng-click="ok()"';
            html += '			ng-show="agg.edit">';
            html += '		    <span class="glyphicon glyphicon-ok text-success"></span></a>';
            html += '	</td>';
            html += '	<td width="10%" valign="top">';
            html += '		<a href="#" class="btn-sm btn-circle" style="margin-left:1px; margin-right:1px;"';
            html += '				ng-click="cancel()"';
            html += '				ng-show="agg.edit">';
            html += '		<span class="glyphicon glyphicon-remove-sign text-danger"></span></a>';
            html += '	</td>';
            html += '</table>';

            element.append(html);
            $compile(element.contents())(scope);

            scope.ok = function()
            {
                delete scope.agg.edit;
                scope.onBlur();
            };

            scope.cancel = function()
            {
                delete scope.agg.edit;
                scope.agg = oldAgg;
            };
        },
        scope: {
            agg: '=',
            descriptors: '=',
            units: '=',
            onBlur: '&'
        }
    }
});

module.directive('aggregatornamedropdown', function ($compile)
{
    return {
        link: function (scope, element, attributes)
        {
            var html = '';
            html += '<div class="dropdown" style="display:inline-block">';
            html += '	<button class="btn btn-default dropdown-toggle" ' +
                    '       type="button" id="aggregatorDropdown" data-toggle="dropdown" aria-haspopup="true" ' +
                    '       aria-expanded="true">';
            html += '		{{agg.name}}';
            html += '		<span class="caret"></span>';
            html += '	</button>';
            html += '	<ul class="dropdown-menu" aria-labelledby="aggregatorDropdown">';
            html += '   <li ng-repeat="descriptor in descriptors">';
			html += '       <a href="#" ng-click="setName(agg, descriptor.name, descriptors)"' +
                    '           title="{{descriptor.description}}" bs-tooltip>';
            html += '           {{ descriptor.name }}';
            html += '       </a>';
            html += '   </li>';
            html += '	</ul>';
            html += '</div>';

            element.append(html);
            $compile(element.contents())(scope);

            scope.setName = function (agg, newName, descriptors)
            {
                scope.clearProperties(agg, descriptors);
                agg.name = newName;
            };

            scope.clearProperties = function (agg, descriptors)
            {
                _.each(descriptors, function (descriptor)
                {
                    _.each(descriptor.properties, function (descriptor)
                    {
                        delete agg[descriptor.name];
                    });
                });
            };
        }
    }
});

module.directive('aggregatorproperties', function ($compile)
{
    return {
        link: function (scope, element, attributes)
        {
            scope.renderHtml = function ()
            {
                html = '';
                _.each(scope.descriptors, function (descriptor)
                {
                    if (descriptor.name == scope.agg.name) {
                        _.each(descriptor.properties, function (property)
                        {
                            if (html.length > 0) {
                                html += "<br/>";
                            }
                            html += scope.getHtml(property);
                        });
                    }
                });

                element.html(html);
                $compile(element.contents())(scope);
            };

            scope.clickFunc = function (agg, newName)
            {
                agg.name = newName;
            };

            scope.$watch("agg.name", function (newValue, oldValue)
            {
                scope.renderHtml();
            });

            scope.setValue = function (obj, path, value)
            {
                path = path.split('.');
                for(var i = 1; i < path.length - 1; i++){
                    if (!obj[path[i]])
                    {
                        obj[path[i]] = {}; // create object if doesn't exist
                    }
                    obj = obj[path[i]];
                }
                obj[path[i]] = value;
            };

            scope.getHtml = function(property, parentName)
            {
                var name = parentName ? parentName + "." + property.name: property.name;
                var html = "";
                var modelName = 'agg.' + name;
                if (property.type == 'boolean') {
                    html += "<span>" + property.name + "</span> ";
                    html += "<input name='" + modelName + "' type='checkbox' ng-init=\"" + modelName + "=" + property.defaultValue + "\" + ng-model='" + modelName + "' >";
                }
                else if (property.type == 'double') {
                    html += "<span>" + property.name + "</span> ";
                    html += "<input  class='small-input' type='text' ng-init=\"" + modelName + "=" + property.defaultValue + "\" + ng-model='" + modelName + "' style='width:30px'>";
                }
                else if (property.type == 'int' || property.type == 'long') {
                    html += "<span>" + property.name + "</span> ";
                    html += "<input name='" + modelName + "' class='small-input' type='text' ng-init=\"" + modelName + "='" + property.defaultValue + "'\" + ng-model='" + modelName + "' value='" + property.defaultValue + "' style='width:30px'>";
                }
                else if (property.type == 'String') {
                    html += "<span>" + property.name + "</span> ";
                    html += "<input class='small-input' type='text' ng-init=\"" + modelName + "='" + property.defaultValue + "'\" ng-model='" + modelName + "' style='width:90px' ng-model='agg." + property.name + "'>";
                }
                else if (property.type == "Object")
                {
                    _.each(property.properties, function (subProperty)
                    {
                        html += scope.getHtml(subProperty, property.name);
                    });
                }
                else if (property.type == 'enum') {
                    scope.values = property.options;
                    scope.setValue(scope.agg, modelName, property.defaultValue);
                    html += "<span>" + property.name + "</span> ";
                    html += '<div class="dropdown" style="display:inline-block">';
                    html += '	<button class="btn btn-default dropdown-toggle" type="button" ' +
                            'id="aggregatorDropdown" data-toggle="dropdown" ' +
                            'aria-haspopup="true" aria-expanded="true" style="font-size: 11px;">';
                    html += '       {{ ' + modelName + ' }}';
                    html += '		<span class="caret"></span>';
                    html += '	</button>';
                    html += '	<ul class="dropdown-menu" aria-labelledby="aggregatorDropdown">';
                    html += '   <li ng-repeat="value in values">';
                    html += '       <a href="#" ng-click="' + modelName + '=value" style="font-size: 11px;">{{ value }}</a>';
                    html += '   </li>';
                    html += '	</ul>';
                    html += '</div>';
                }
                else {
                    scope.$parent.alert("Invalid aggregator property type. Property name: '" +
                            property.name + "'. Type: '" + property.type + "'.");
                }

                return html;
            };


            scope.renderHtml();
        }
    }
});
