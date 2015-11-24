module.controller('PasteQueryCtrl', ['$scope', '$modalInstance', PasteQueryCtrl]);

function PasteQueryCtrl($scope, $modalInstance) {
	$scope.DEFAULT_SAVE_AS = "Save As";
	$scope.DEFAULT_QUERY = "Paste Query";

	$scope.init = function () {
		$scope.target = {};
		$scope.targetBlur();
		$scope.errors = validateTarget($scope.target);
	};

	$scope.clearSaveAs = function () {
		if ($scope.isSaveAsEmptyOrDefault()) {
			$scope.target.save_as = "";
		}
	};

	$scope.clearQuery = function () {
		if ($scope.isQueryEmptyOrDefault()) {
			$scope.target.query = "";
		}
	};

	$scope.ok = function () {
		var result = {};
		result.save_as = $scope.target.save_as;
		result.query = JSON.parse($scope.target.query);
		$modalInstance.close(result);
	};

	$scope.cancel = function () {
		$modalInstance.dismiss('cancel');
	};

	$scope.isQueryEmptyOrDefault = function () {
		return !$scope.target.query || $scope.target.query == $scope.DEFAULT_QUERY;
	};

	$scope.isSaveAsEmptyOrDefault = function () {
		return !$scope.target.save_as || $scope.target.save_as == $scope.DEFAULT_SAVE_AS;
	};

	$scope.suggestSaveAs = function () {
		if (!$scope.isQueryEmptyOrDefault() && $scope.isSaveAsEmptyOrDefault()) {
			try {
				query = JSON.parse($scope.target.query);
				if (query.metrics[0] && query.metrics[0].name) {
					$scope.target.save_as = query.metrics[0].name + "_rollup";
				}
			}
			catch (err) {
				$scope.errors.query = "Invalid JSON."
			}
		}
		$scope.targetChanged();
	};

	$scope.targetChanged = function () {
		$scope.errors = validateTarget($scope.target);
		if (!_.isEqual($scope.oldTarget, $scope.target) && _.isEmpty($scope.errors)) {
			$scope.oldTarget = angular.copy($scope.target);
		}
	};

	$scope.targetBlur = function () {
		if (!$scope.target.query) {
			$scope.target.query = $scope.DEFAULT_QUERY;
		}
		if (!$scope.target.save_as) {
			$scope.target.save_as = $scope.DEFAULT_SAVE_AS;
		}
	};

	$scope.hasErrors = function () {
		return !_.isEmpty($scope.errors);
	};

	//////////////////////////////
	// VALIDATION
	//////////////////////////////

	function validateTarget(target) {
		var errs = {};

		if ($scope.isSaveAsEmptyOrDefault()) {
			errs.save_as = "You must supply a new metric name.";
		}

		if ($scope.isQueryEmptyOrDefault()) {
			errs.query = "You must paste in a query.";
		}
		else {
			try {
				JSON.parse($scope.target.query);
			}
			catch (err) {
				errs.query = "Invalid JSON";
			}
		}

		return errs;
	}

}