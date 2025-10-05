use chrono::{DateTime, Utc};
use kernel::model::{
    checkout::{Checkout, CheckoutBook},
    id::{BookId, CheckoutId, UserId},
};

// 貸出状態を確認するための型
// 蔵書が存在する場合はこの型にはまるレコードが存在し、
// その蔵書が貸出中の場合はcheckout_idおよびuser_idがNoneではない値になる。
// 蔵書が貸出中でない場合はcheckout_idもuser_idもNone
pub struct CheckoutStateRow {
    pub book_id: BookId,
    pub checkout_id: Option<CheckoutId>,
    pub user_id: Option<UserId>,
}

// 貸出中の一覧を取得する再に使う型
pub struct CheckoutRow {
    pub checkout_id: CheckoutId,
    pub book_id: BookId,
    pub user_id: UserId,
    pub checked_out_at: DateTime<Utc>,
    pub title: String,
    pub author: String,
    pub isbn: String,
}

impl From<CheckoutRow> for Checkout {
    fn from(value: CheckoutRow) -> Self {
        let CheckoutRow {
            checkout_id,
            book_id,
            user_id,
            checked_out_at,
            title,
            author,
            isbn,
        } = value;
        Checkout {
            id: checkout_id,
            checked_out_by: user_id,
            checked_out_at,
            returned_at: None,
            book: CheckoutBook {
                book_id,
                title,
                author,
                isbn,
            },
        }
    }
}

// 返却済みの貸出一覧を取得する際に使う型
pub struct ReturnedCheckoutRow {
    pub checkout_id: CheckoutId,
    pub book_id: BookId,
    pub user_id: UserId,
    pub checked_out_at: DateTime<Utc>,
    pub returned_at: DateTime<Utc>,
    pub title: String,
    pub author: String,
    pub isbn: String,
}

impl From<ReturnedCheckoutRow> for Checkout {
    fn from(value: ReturnedCheckoutRow) -> Self {
        let ReturnedCheckoutRow {
            checkout_id,
            book_id,
            user_id,
            checked_out_at,
            returned_at,
            title,
            author,
            isbn,
        } = value;
        Checkout {
            id: checkout_id,
            checked_out_by: user_id,
            checked_out_at,
            returned_at: Some(returned_at),
            book: CheckoutBook {
                book_id,
                title,
                author,
                isbn,
            },
        }
    }
}
