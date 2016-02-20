var module = angular.module('simpleRollupApp', ['mgcrea.ngStrap', 'mgcrea.ngStrap.tooltip']);

module.directive('editable', function () {
	return {
		restrict: 'E',
		scope: {model: '='},
		replace: false,
		template: '<span>' +
		'<input type="text" ng-model="model"  ng-blur="$parent.onBlur($parent.task)" ng-show="edit" my-blur="edit"></input>' +
		'<a href="" ng-show="!edit" style="color:black">{{model}}</a>' +
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
			'<input type="text" ng-show="edit" my-blur="edit" ng-blur="suggestSaveAs(task)" class="input-large tight-form-input ng-pristine ng-valid ng-touched" ng-model="task.metric_name" myblur="edit" spellcheck="false" bs-typeahead bs-options="metric for metric in suggestMetrics(task.metric_name)" placeholder="<metric name>" min-length="0" limit="METRIC_NAME_LIST_MAX_LENGTH" ng-change="targetBlur()" ng-blur="suggestSaveAs()" data-provide="typeahead"	ng-focus autofocus></input>' +
		//'<input type="text" ng-model="task.metric_name" ng-show="edit" my-blur="edit">{{model}}</input>' +
		'<a href="" ng-show="!edit" style="color:black">{{task.metric_name}}</a>' +
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

