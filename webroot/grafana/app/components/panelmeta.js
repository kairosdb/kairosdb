define([],
	function () {
		"use strict";

		function PanelMeta(options) {
			this.description = options.description;
			this.fullscreen = options.fullscreen;
			this.menu = [];
			this.editorTabs = [];
			this.extendedMenu = [];

			if (options.fullscreen) {
				this.addMenuItem('view', 'icon-eye-open', 'toggleFullscreen(false); dismiss();');
			}

			this.addMenuItem('edit', 'icon-cog', 'editPanel(); dismiss();');
			this.addMenuItem('duplicate', 'icon-copy', 'duplicatePanel()');
			this.addMenuItem('share', 'icon-share', 'sharePanel(); dismiss();');

			this.addEditorTab('General', 'app/partials/panelgeneral.html');

			if (options.metricsEditor) {
				this.addEditorTab('Metrics', 'app/partials/metrics.html');
			}

			this.addExtendedMenuItem('Panel JSON', '', 'editPanelJson(); dismiss();');
		}

		PanelMeta.prototype.addMenuItem = function (text, icon, click) {
			this.menu.push({text: text, icon: icon, click: click});
		};

		PanelMeta.prototype.addExtendedMenuItem = function (text, icon, click) {
			this.extendedMenu.push({text: text, icon: icon, click: click});
		};

		PanelMeta.prototype.addEditorTab = function (title, src) {
			this.editorTabs.push({title: title, src: src});
		};

		return PanelMeta;

	});
