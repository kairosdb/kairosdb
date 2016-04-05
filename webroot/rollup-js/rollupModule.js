var module = angular.module('rollupApp',
	['mgcrea.ngStrap',
		'mgcrea.ngStrap.tooltip',
		'ui.bootstrap.modal',
		'template/modal/backdrop.html',
		'template/modal/window.html']);

module.directive('editable', function () {
	return {
		restrict: 'E',
		scope: {model: '='},
		replace: false,
		template: '<span>' +
		'<input type="text" ' +
		'	style="width:100%; ' +
		'	padding:0; ' +
		'	line-height:16px" ' +
		'	ng-model="model"  ' +
		'	ng-blur="$parent.onBlur($parent.task)" ' +
		'	ng-show="edit" ' +
		'	my-blur="edit">' +
		'</input>' +

		'<a href=""ng-show="!edit" ' +
				'ng-class="model.indexOf(\'<\') == 0 ? \'gray\' : \'black\'"' +
		'>{{model}}' +
		'</a>' +
		'</span>',
		link: function (scope, element, attrs) {
			scope.edit = false;
			element.bind('click', function () {
				scope.$apply(scope.edit = true);
				element.find('input').focus();

				inputField = element.find('input').get(0);
				if (inputField.value.indexOf("<") === 0) {
					// Remove text from text field since its just placeholder text
					inputField.value = '';
				}
			});
		}
	};
});

module.directive('autocompleteeditable', function () {
	return {
		restrict: 'E',
		replace: false,
		template: '<span>' +
		'<input ' +
		'type="text"' +
		'ng-show="edit"' +
		'my-blur="edit"' +
		'ng-blur="suggestSaveAs(task)"' +
		'class="input-large tight-form-input ng-pristine ng-valid ng-touched"' +
		'ng-model="task.metric_name"' +
		'myblur="edit" spellcheck="false"' +
		'bs-typeahead bs-options="metric for metric in suggestMetrics(task.metric_name)"' +
		'placeholder="<metric name>"' +
		'min-length="0"' +
		'limit="METRIC_NAME_LIST_MAX_LENGTH"' +
		'ng-change="targetBlur()"' +
		'ng-blur="suggestSaveAs()"' +
		'data-provide="typeahead"' +
		'style="width:100%; padding:0; line-height:16px;"' +
		'ng-focus autofocus>' +
		'</input>' +

		'<a href="" ' +
		'	ng-show="!edit" ' +
		'	ng-class="task.metric_name.indexOf(\'<\') == 0 ? \'gray\' : \'black\'"' +
		'>{{task.metric_name}}</a>' +
		'</span>',
		link: function (scope, element, attrs) {
			scope.edit = false;
			element.bind('click', function () {
				scope.$apply(scope.edit = true);
				element.find('input').focus();

				inputField = element.find('input').get(0);
				if (inputField.value.indexOf("<") === 0) {
					// Remove text from text field since its just placeholder text
					inputField.value = '';
				}
			});
		}
	};
});

//blur directive
module.directive('myBlur', function () {
	return {
		restrict: 'A',
		link: function (scope, element, attr) {
			element.bind('blur', function () {
				scope.edit = false;
				scope.$apply();
			});
		}
	};
});

module.directive('customPopover', function () {
	return {
		restrict: 'A',
		template: '<a href="#"><span class="glyphicon glyphicon-question-sign" aria-hidden="true"></span></a>',
		link: function (scope, el, attrs) {
			$(el).popover({
				trigger: 'focus',
				html: true,
				content: attrs.popoverHtml,
				placement: attrs.popoverPlacement
			});
		}
	};
});

module.directive('focusOnShow', function ($timeout) {
	return {
		restrict: 'A',
		link: function ($scope, $element, $attr) {
			if ($attr.ngShow) {
				$scope.$watch($attr.ngShow, function (newValue) {
					if (newValue) {
						$timeout(function () {
							$element.focus();
						}, 0);
					}
				})
			}
			if ($attr.ngHide) {
				$scope.$watch($attr.ngHide, function (newValue) {
					if (!newValue) {
						$timeout(function () {
							$element.focus();
						}, 0);
					}
				})
			}

		}
	};
});

// This is needed by dynamically created elements. The regular tooltip mechanism
// does not work for dynamic elements
module.directive('bsTooltip', function () {
	return {
		restrict: 'A',
		link: function (scope, element, attrs) {
			$(element).hover(function () {
				// on mouseenter
				$(element).tooltip('show');
			}, function () {
				// on mouseleave
				$(element).tooltip('hide');
			});
		}
	};
});

module.directive('autocompleteWithButtons', function ($compile) {
	var template = '' +
		'<table width="100%">' +
		'		<tr>' +
		'			<td>' +
		'			<span ng-show="!model.edit" style="margin: 0 10px 0 0">{{model.group_by_values}}</span>' +
		'			<input type="text" style="width:85px; padding:0; line-height:16px"' +
		'				focus-on-show ' +
		'				class="input-small tight-form-input ' +
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
		'					ng-show="!model.edit">' +
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
		'						ng-show="model.edit">' +
		'				<span class="glyphicon glyphicon-remove-sign text-danger"></span></a>' +
		'			</td>' +
		'		</tr>' +
		'</table>';

	var linker = function(scope, element, attributes) {
		element.html(template).show();

		$compile(element.contents())(scope);

		scope.cancelGroupValues = function(model){
			model.edit=false;
			model.group_by_value_suggest = "";
		};

		scope.removeGroupValues = function(model){
			if (model.group_by_values){
				model.group_by_values = "";
			}
		};

		scope.setGroupValues = function(model){
			model.edit=false;
			if (!model.group_by_values){
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
			model:'=',
			list: '&',
			onBlur: '&'
		}
	};
});


module.directive('autocompleteTags', function ($compile) {
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
		'			</table> '+
		'			<input type="text" style="width:85px; padding:0; line-height:16px"' +
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
		'					ng-show="!model.tagEdit">' +
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
		'					ng-show="model.tags && !model.tagEdit">' +
		'				<span class="glyphicon glyphicon-remove text-danger"></span></a>' +
		'					<a href="#" class="btn-sm btn-circle" style="margin-left:1px; margin-right:1px;"' +
		'						ng-click="cancelTagValues(model)"' +
		'						ng-show="model.tagEdit">' +
		'				<span class="glyphicon glyphicon-remove-sign text-danger"></span></a>' +
		'			</td>' +
		'		</tr>' +
		'</table>';

	var linker = function(scope, element, attributes) {
		element.html(template).show();

		$compile(element.contents())(scope);

		scope.cancelTagValues = function(model){
			model.tagEdit=false;

			delete model.currentTagKey;
			delete model.currentTagValue
		};

		scope.removeTag = function(model){
			delete model.tags;
		};

		scope.setTag = function(model){
			model.tagEdit=false;
			if (!model.tags) {
				model.tags = {};
			}

			var existingTag = [];
			if (model.tags[model.currentTagKey]){
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
			model:'=',
			keyList: '&',
			valueList: '&',
			onBlur: '&'
		}
	};
});
