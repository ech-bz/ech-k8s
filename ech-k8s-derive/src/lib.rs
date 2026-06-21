use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Meta, Token, parse_macro_input, punctuated::Punctuated};

#[proc_macro_derive(Component, attributes(component))]
pub fn derive_component(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_component(input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn expand_component(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name_attr = find_component_name(&input.attrs)?;
    let struct_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let component_params: Vec<&syn::Ident> = input
        .generics
        .params
        .iter()
        .filter_map(|p| {
            if let syn::GenericParam::Type(tp) = p {
                if has_component_bound(&tp.bounds) {
                    Some(&tp.ident)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    enum FieldKind { Comp, Value }

    let (value_fields, value_idents, ordered) = match &input.data {
        syn::Data::Struct(ds) => match &ds.fields {
            syn::Fields::Named(fields) => {
                let mut vals = Vec::new();
                let mut labels = Vec::new();
                let mut ord: Vec<(FieldKind, proc_macro2::TokenStream)> = Vec::new();
                for f in &fields.named {
                    let Some(id) = &f.ident else { continue };
                    let accessor = quote! { &self.#id };
                    if field_has_component_param(&f.ty, &component_params) {
                        ord.push((FieldKind::Comp, accessor));
                    } else {
                        vals.push(accessor.clone());
                        labels.push(id.to_string());
                        ord.push((FieldKind::Value, accessor));
                    }
                }
                (vals, labels, ord)
            }
            syn::Fields::Unnamed(fields) => {
                let mut vals = Vec::new();
                let mut labels = Vec::new();
                let mut ord: Vec<(FieldKind, proc_macro2::TokenStream)> = Vec::new();
                for (i, f) in fields.unnamed.iter().enumerate() {
                    let idx = syn::Index::from(i);
                    let accessor = quote! { &self.#idx };
                    if field_has_component_param(&f.ty, &component_params) {
                        ord.push((FieldKind::Comp, accessor));
                    } else {
                        vals.push(accessor.clone());
                        labels.push(i.to_string());
                        ord.push((FieldKind::Value, accessor));
                    }
                }
                (vals, labels, ord)
            }
            syn::Fields::Unit => (Vec::new(), Vec::new(), Vec::new()),
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "Component can only be derived for structs",
            ));
        }
    };

    let instance_fields: Vec<_> = ordered.iter().map(|(kind, accessor)| {
        match kind {
            FieldKind::Comp => quote! {
                name = (#accessor).instance_name(name)?;
            },
            FieldKind::Value => quote! {
                name.push('-');
                name.push_str(&(#accessor).to_string());
            },
        }
    }).collect();

    let instance_name_body = quote! {
        let mut name = owner.as_ref().to_string();
        name.push('-');
        name.push_str(Self::NAME);
        #(#instance_fields)*
        Ok(name)
    };

    let labels_body = quote! {
        let mut labels: ::std::collections::BTreeMap<String, String> = ::std::collections::BTreeMap::from([
            (String::from("ech.bz/owner"), owner.as_ref().to_string()),
            (String::from("ech.bz/component"), Self::NAME.to_string()),
        ]);
        #(
            labels.insert(
                format!("ech.bz/{}", #value_idents),
                (#value_fields).to_string(),
            );
        )*
        Ok(labels)
    };

    let expanded = quote! {
        impl #impl_generics ::ech_k8s::Component for #struct_name #ty_generics #where_clause {
            const NAME: &'static str = #name_attr;

            fn instance_name(
                &self,
                owner: impl AsRef<str>,
            ) -> ::std::result::Result<String, ::ech_k8s::ReconcilerMetaError> {
                #instance_name_body
            }

            fn labels(
                &self,
                owner: impl AsRef<str>,
            ) -> ::std::result::Result<::std::collections::BTreeMap<String, String>, ::ech_k8s::ReconcilerMetaError> {
                #labels_body
            }

            fn selector(owner: impl AsRef<str>) -> String {
                format!(
                    "ech.bz/owner={},ech.bz/component={}",
                    owner.as_ref(),
                    Self::NAME,
                )
            }
        }
    };

    Ok(expanded)
}

fn find_component_name(attrs: &[syn::Attribute]) -> syn::Result<String> {
    for attr in attrs {
        if !attr.path().is_ident("component") {
            continue;
        }
        let meta: Meta = attr.parse_args()?;
        if let Meta::NameValue(nv) = meta {
            if nv.path.is_ident("name") {
                if let syn::Expr::Lit(lit) = &nv.value {
                    if let syn::Lit::Str(s) = &lit.lit {
                        return Ok(s.value());
                    }
                }
            }
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "missing #[component(name = \"...\")] attribute",
    ))
}

fn field_has_component_param(ty: &syn::Type, component_params: &[&syn::Ident]) -> bool {
    if let syn::Type::Path(tp) = ty {
        if let Some(segment) = tp.path.segments.last() {
            return component_params.iter().any(|p| *p == &segment.ident);
        }
    }
    false
}

fn has_component_bound(bounds: &Punctuated<syn::TypeParamBound, Token![+]>) -> bool {
    bounds.iter().any(|b| {
        if let syn::TypeParamBound::Trait(tb) = b {
            tb.path
                .segments
                .last()
                .map_or(false, |s| s.ident == "Component")
        } else {
            false
        }
    })
}
