#[macro_use]
extern crate stdweb;

extern crate hyper;

use stdweb::web::{
    IEventTarget,
    IElement,
    IHtmlElement,
    INode,
    HtmlElement,
    Element,
    document,
    window
};

fn main() {
    stdweb::initialize();
    let table = document().query_selector("#table")?;
    
    stdweb::event_loop();
}
