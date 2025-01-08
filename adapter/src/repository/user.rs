use async_trait::async_trait;
use derive_new::new;
use kernel::{
    model::{
        id::UserId,
        user::{
            event::{CreateUser, DeleteUser, UpdateUserPassword, UpdateUserRole},
            User,
        },
    },
    repository::user::UserRepository,
};
use shared::error::{AppError, AppResult};

use crate::database::{model::user::UserRow, ConnectionPool};

#[derive(new)]
pub struct UserRepositoryImpl {
    db: ConnectionPool,
}

#[async_trait]
impl UserRepository for UserRepositoryImpl {
    async fn find_current_user(&self, current_user_id: UserId) -> AppResult<Option<User>> {
        let row = sqlx::query_as!(
            UserRow,
            r#"
                SELECT
                    users.user_id,
                    users.name,
                    users.email,
                    roles.name AS role_name,
                    users.created_at,
                    users.updated_at
                FROM users
                INNER JOIN roles USING(role_id)
                WHERE users.user_id = $1
            "#,
            current_user_id as _
        )
        .fetch_optional(self.db.inner_ref())
        .await
        .map_err(AppError::SpecificOperationError)?;
        match row {
            Some(r) => Ok(Some(User::try_from(r)?)),
            None => Ok(None),
        }
    }

    async fn find_all(&self) -> AppResult<Vec<User>> {
        todo!()
    }

    async fn create(&self, event: CreateUser) -> AppResult<User> {
        todo!()
    }

    async fn update_password(&self, event: UpdateUserPassword) -> AppResult<()> {
        todo!()
    }

    async fn update_role(&self, event: UpdateUserRole) -> AppResult<()> {
        todo!()
    }

    async fn delete(&self, event: DeleteUser) -> AppResult<()> {
        todo!()
    }
}
