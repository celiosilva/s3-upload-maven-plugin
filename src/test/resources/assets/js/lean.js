/**
 * Namespace para o módulo Lean
 */
var Lean = (Lean || {});

/**
 * Classe para manipulação do Modal do bootstrap
 */
Lean.Modal = function(modalDivId) {

    /**
     * Obtem o html para determinada view e inclui dentro do modal configurado
     */
    this.get = function(url) {
        $.get(url, function(data) {
            $(modalDivId + ' .modal-content').html(data);
        });
    };

    /**
     * Faz o post do form e o replace do modal com o resultado esperado
     */
    this.postUrl = function(url, formId) {
        $.post(url, $(formId).serialize(), function(data) {
            $(modalDivId + ' .modal-content').html(data);
        });
    };

    /**
     * Faz o post do form e o replace do modal com o resultado esperado
     */
    this.post = function(formId) {
        $.post($(formId).attr('action'), $(formId).serialize(), function(data) {
            $(modalDivId + ' .modal-content').html(data);
        });
    };

};

/**
 * Classes para componentes numéricos
 */
Lean.Campos = function() {
    this.decimal = function(inputId) {
        $(inputId).mask("00.000.000,00", {
            reverse : true,
            maxlength : true
        }).css("text-align", "right");

    };

    this.iniciarContador = function(htmlElement) {
        $(htmlElement + '[data-contador-para]').each(function() {
            var contador = this;
            var input = $(contador).data('contador-para');
            contar(input, contador);
            $(input).keyup(function() {
                contar(input, contador);
            });
        });
    };

    function contar(input, counterId) {
        var currentLenght = $(input).val().length;
        var maxLenght = $(input).attr("maxLength");
        var counterLenght = maxLenght - currentLenght;
        $(counterId).text(counterLenght);
    }
    ;

};

function contador(e){
  var maximo = $(e).attr('maxlength');
  var length = $(e).val().length;
  var length = maximo - length;
  $(e).next().text(length + ' caracteres restantes.');
}

/**
 * Faz uma simples chamada post para fazer um reload da pagina
 *
 * @param url
 * @param div
 */
function chamadaPost(url, div) {
    $.post(url, function(data) {
        $(div).html(data);
    });
}

/**
 * Captura o click de elementos que possuam que possuam atributo data-manipulate
 * e data-class, adicionando a classe do elemento manipulado
 */
$('[data-manipulate]').click(function() {
    $($(this).data('manipulate')).toggleClass($(this).data('class'));
})

/**
 * Funções adicionais
 */
var menuVisivelXS = false;
// oculta menu quando browser menor que 768 width
$("#menu-toggle").click(function(e) {
    if ($(window).width() < 1026) {
        menuVisivelXS = true;
        $("#wrapper").addClass("toggled");
    } else {
        menuVisivelXS = false;
    }
});

$('.fechar-menu').click(function() {
        $("#wrapper").removeClass("toggled");
        menuVisivelXS = false;
    });

var isSafari =
    navigator.userAgent.indexOf('Safari') != -1 && navigator.userAgent.indexOf('Chrome') == -1;

// aplica sombra no header quando a página descer
$(document).ready(function() {
    $(window).scroll(function() {
        if ($(window).scrollTop() > 0) {
            $(".flutuar").addClass('sombra');
        } else {
            $(".flutuar").removeClass('sombra');
        }
    });
    // quando menu aberto e browser for redimensionado, corrige menu e conteudo
    $(window).on("load resize", function() {

        // corrigir menu superior no safari
        if(isSafari){
            if ( $(window).width() > 1026){
                $('.navbar-fixed-top').css('width', $(window).width() - 232 + 'px');
            } else{
                $('.navbar-fixed-top').css('width', '100%');
            }
        }

        if ($(window).width() > 1026 && menuVisivelXS) {
            $("#wrapper").removeClass("toggled");
            menuVisivelXS = false;
        }

        if ($(".copiar-altura").length) {
            var max = 0;
            var els = [];
            $('.copiar-altura').each(function(i, e) {
                els.push(e);
                if ($(e).height() > max) {
                    max = $(e).height();
                }
            });
            $.each(els, function(i, e) {
                $(e).css('height', max);
            });
        }
    });
});

function dataInvalida(data){
    return (data.length !== 10) || (parseInt(data.split('/')[0]) > 31) || (parseInt(data.split('/')[1]) > 12);
}

function emailInvalido(email){
    console.log(email)

    return !/^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/.test(email);
}
