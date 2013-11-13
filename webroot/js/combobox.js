/*!
 * Copyright Ben Olson (https://github.com/bseth99/jquery-ui-extensions)
 * jQuery UI ComboBox @VERSION
 *
 *  Adapted from Jörn Zaefferer original implementation at
 *  http://www.learningjquery.com/2010/06/a-jquery-ui-combobox-under-the-hood
 *
 *  And the demo at
 *  http://jqueryui.com/autocomplete/#combobox
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *
 */

/**
 * Modified by Jeff Sabin:
 *  <li>to allow the source to specified so an AJAX call can be made.
 *  <li>invalid text is not removed
 *  <li>value() return the text in the input field if it is not valid
 */

(function( $, undefined ) {

	$.widget( "ui.combobox", {

		options: {
			source: null,
			MAX_DROP_DOWN_LENGTH: 200
		},

		version: "@VERSION",

		widgetEventPrefix: "combobox",

		uiCombo: null,
		uiInput: null,
		_wasOpen: false,

		_create: function() {

			var self = this,
				select = this.element.hide(),
				input, wrapper;

			input = this.uiInput =
				$( "<input />" )
					.insertAfter(select)
					.addClass("ui-widget ui-widget-content ui-corner-left ui-combobox-input")
					.val( select.children(':selected').text() );

			wrapper = this.uiCombo =
				input.wrap( '<span>' )
					.parent()
					.addClass( 'ui-combobox' )
					.insertAfter( select );

			input
				.autocomplete({

					delay: 0,
					minLength: 0,

					appendTo: wrapper,
					source: $.proxy( this, "_linkSelectList" )
				});

			$( "<button>" )
				.attr( "tabIndex", -1 )
				.attr( "type", "button" )
				.insertAfter( input )
				.button({
					icons: {
						primary: "ui-icon-triangle-1-s"
					},
					text: false
				})
				.removeClass( "ui-corner-all" )
				.addClass( "ui-corner-right ui-button-icon ui-combobox-button" );


			// Our items have HTML tags.  The default rendering uses text()
			// to set the content of the <a> tag.  We need html().
			input.data( "ui-autocomplete" )._renderItem = function( ul, item ) {

				return $( "<li>" )
					.append( $( "<a>" ).html( item.label ) )
					.appendTo( ul );

			};

			this._on( this._events );

		},

		_linkSelectList: function (request, response) {
			var optionElement = this.element;
			var maxDropDown = this.options.MAX_DROP_DOWN_LENGTH;

			this.options.source(request, function (items) {
				var matcher = new RegExp($.ui.autocomplete.escapeRegex(request.term), 'i');

				optionElement.empty();
				var itemsThatMatch = 0;
				var listLimited = false;
				$.each(items, function (index, item) {
					if (matcher.test(item)) {
						itemsThatMatch++;
						if (itemsThatMatch <= maxDropDown) {
							optionElement.append($('<option class="myOption"/>').val(item).text(item));
						}
						else
							listLimited = true;
					}
				});

				if (listLimited) {
					optionElement.append($("<option />").val("[maxItems]").text("<strong>[showing " + maxDropDown + " of " + itemsThatMatch + "]</strong>"));
				}


				response(optionElement.children('option').map(function () {
					var text = $(this).text();

						return {
							label: text.replace(
								new RegExp(
									"(?![^&;]+;)(?!<[^<>]*)(" +
										$.ui.autocomplete.escapeRegex(request.term) +
										")(?![^<>]*>)(?![^&;]+;)", "gi"),
								"<strong>$1</strong>"),
							value: text,
							option: this
						};
				}));
			});
		},


		_events: {

			"autocompletechange input" : function(event, ui) {

				var $el = $(event.currentTarget);

				if ( !ui.item ) {

					var matcher = new RegExp( "^" + $.ui.autocomplete.escapeRegex( $el.val() ) + "$", "i" ),
						valid = false;

					this.element.children( "option" ).each(function() {
						if ( $( this ).text().match( matcher ) ) {
							this.selected = valid = true;
							return false;
						}
					});
				}

				this._trigger( "change", event, {
					item: ui.item ? ui.item.option : null
				});

			},

			"autocompleteselect input": function (event, ui) {

				if (ui.item.option.value == "[maxItems]") {
					// do not select item
					ui.item.option.selected = false;
					return false;
				}
				else {
					ui.item.option.selected = true;
					this._trigger("select", event, {
						item: ui.item.option
					});
				}

			},

			"autocompleteopen input": function ( event, ui ) {

				this.uiCombo.children('.ui-autocomplete')
					.outerWidth(this.uiCombo.outerWidth(true));
			},

			"mousedown .ui-combobox-button" : function ( event ) {
				this._wasOpen = this.uiInput.autocomplete("widget").is(":visible");
			},

			"hover .ui-combobox-button": function( event ) {
				alert("here");
			},

			"click .ui-combobox-button" : function( event ) {

				this.uiInput.focus();

				// close if already visible
				if (this._wasOpen) {
					this._trigger("change");
					return;
				}

				// pass empty string as value to search for, displaying all results
				this.uiInput.autocomplete("search", "");

			}
		},

		value: function ( newVal ) {
			var select = this.element,
				valid = false,
				selected;

			if ( !arguments.length ) {
				selected = select.children( ":selected" );

				return selected.length > 0 ? selected.val() : this.uiInput.val().length < 0 ? null : this.uiInput.val();
			}

			select.prop('selectedIndex', -1);
			select.children('option').each(function() {
				if ( this.value == newVal ) {
					this.selected = valid = true;
					return false;
				}
			});

			if ( valid ) {
				this.uiInput.val(select.children(':selected').text());
			}
		},

		_destroy: function () {
			this.element.show();
			this.uiCombo.replaceWith( this.element );
		},

		widget: function () {
			return this.uiCombo;
		},

		_getCreateEventData: function() {

			return {
				select: this.element,
				combo: this.uiCombo,
				input: this.uiInput
			};
		}

	});


}(jQuery));